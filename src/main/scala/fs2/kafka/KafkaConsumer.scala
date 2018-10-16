package fs2.kafka

import java.time.Duration
import java.util

import cats.data.{Chain, NonEmptyList}
import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.syntax.concurrent._
import cats.effect.{ConcurrentEffect, Fiber, Timer, _}
import cats.instances.unit._
import cats.syntax.apply._
import cats.syntax.flatMap._
import cats.syntax.monadError._
import cats.syntax.semigroup._
import cats.syntax.traverse._
import fs2.concurrent.Queue
import fs2.kafka.internal.syntax._
import fs2.{Sink, Stream}
import org.apache.kafka.clients.consumer.{KafkaConsumer => KConsumer, _}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._

sealed abstract class KafkaConsumer[F[_], K, V] {
  def stream(sink: Sink[F, CommittableMessage[F, K, V]]): Stream[F, Unit]

  def subscribe(topics: NonEmptyList[String]): Stream[F, Unit]

  def fiber: Fiber[F, Unit]
}

object KafkaConsumer {
  private[this] final case class State[F[_], K, V](
    fetches: Chain[Deferred[F, Stream[F, CommittableMessage[F, K, V]]]],
    records: Chain[CommittableMessage[F, K, V]],
    subscribed: Boolean
  ) {
    def withFetch(fetch: Deferred[F, Stream[F, CommittableMessage[F, K, V]]]): State[F, K, V] =
      copy(fetches = fetches append fetch)

    def withoutFetches: State[F, K, V] =
      copy(fetches = Chain.empty)

    def withRecords(records: Chain[CommittableMessage[F, K, V]]): State[F, K, V] =
      copy(records = this.records ++ records)

    def withoutRecords: State[F, K, V] =
      copy(records = Chain.empty)

    def asSubscribed: State[F, K, V] =
      copy(subscribed = true)
  }

  private[this] object State {
    def empty[F[_], K, V]: State[F, K, V] =
      State(
        fetches = Chain.empty,
        records = Chain.empty,
        subscribed = false
      )
  }

  private[this] sealed abstract class Request[F[_], K, V]

  private[this] final case class Poll[F[_], K, V]() extends Request[F, K, V]

  private[this] final case class Subscribe[F[_], K, V](topics: NonEmptyList[String])
      extends Request[F, K, V]

  private[this] final case class Fetch[F[_], K, V](
    deferred: Deferred[F, Stream[F, CommittableMessage[F, K, V]]]
  ) extends Request[F, K, V]

  private[this] final case class Commit[F[_], K, V](
    offsets: Map[TopicPartition, OffsetAndMetadata],
    deferred: Deferred[F, Either[Throwable, Unit]]
  ) extends Request[F, K, V]

  private[this] final case class ConsumerActor[F[_], K, V](
    settings: ConsumerSettings[K, V],
    ref: Ref[F, State[F, K, V]],
    requests: Queue[F, Request[F, K, V]],
    consumer: Consumer[K, V]
  )(
    implicit F: ConcurrentEffect[F],
    context: ContextShift[F],
    timer: Timer[F]
  ) {
    private def subscribe(topics: NonEmptyList[String]): F[Unit] =
      context.evalOn(settings.executionContext) {
        F.delay(consumer.subscribe(topics.toList.asJava))
      } *> ref.update(_.asSubscribed)

    private def fetch(deferred: Deferred[F, Stream[F, CommittableMessage[F, K, V]]]): F[Unit] =
      ref.update(_.withFetch(deferred))

    private def commit(
      offsets: Map[TopicPartition, OffsetAndMetadata],
      deferred: Deferred[F, Either[Throwable, Unit]]
    ): F[Unit] =
      context.evalOn(settings.executionContext) {
        F.delay {
          consumer.commitAsync(
            offsets.asJava,
            new OffsetCommitCallback {
              override def onComplete(
                offsets: util.Map[TopicPartition, OffsetAndMetadata],
                exception: Exception
              ): Unit = {
                val result = Option(exception).toLeft(())
                val complete = deferred.complete(result)
                F.runAsync(complete)(_ => IO.unit).unsafeRunSync()
              }
            }
          )
        }
      }

    private def message(
      record: ConsumerRecord[K, V],
      partition: TopicPartition
    ): CommittableMessage[F, K, V] =
      CommittableMessage(
        record = record,
        committableOffset = CommittableOffset(
          topicPartition = partition,
          offsetAndMetadata = new OffsetAndMetadata(record.offset + 1L),
          commit = offsets =>
            Deferred[F, Either[Throwable, Unit]].flatMap { deferred =>
              requests.enqueue1(Commit(offsets, deferred)) *>
                F.race(timer.sleep(settings.commitTimeout), deferred.get.rethrow)
                  .flatMap {
                    case Right(_) => F.unit
                    case Left(_) =>
                      F.raiseError[Unit] {
                        new CommitTimeoutException(
                          settings.commitTimeout,
                          offsets
                        )
                      }
                  }
          }
        )
      )

    private def records(batch: ConsumerRecords[K, V]): Chain[CommittableMessage[F, K, V]] =
      if (batch.isEmpty) Chain.empty
      else {
        var messages = Chain.empty[CommittableMessage[F, K, V]]
        val partitions = batch.partitions.iterator

        while (partitions.hasNext) {
          val partition = partitions.next()
          val records = batch.records(partition).iterator

          while (records.hasNext) {
            messages = messages append message(records.next(), partition)
          }
        }

        messages
      }

    private val poll: F[Unit] = {
      ref.get.flatMap { state =>
        if (state.subscribed) {
          context
            .evalOn(settings.executionContext) {
              F.delay {
                val assignment = consumer.assignment()
                if (state.fetches.isEmpty || state.records.nonEmpty) {
                  consumer.pause(assignment)
                  consumer.poll(Duration.ZERO)
                } else {
                  consumer.resume(assignment)
                  consumer.poll(settings.pollTimeout.asJava)
                }
              }
            }
            .flatMap { batch =>
              def newRecords = records(batch)

              if (state.fetches.isEmpty) {
                if (batch.isEmpty) F.unit
                else ref.update(_.withRecords(newRecords))
              } else {
                val allRecords = state.records ++ newRecords
                if (allRecords.nonEmpty) {
                  val allRecordsStream = Stream.fromIterator(allRecords.iterator)
                  state.fetches.traverse(_.complete(allRecordsStream)) *>
                    ref.set(state.withoutFetches.withoutRecords)
                } else F.unit
              }
            }
        } else F.unit
      }
    }

    def handle(request: Request[F, K, V]): F[Unit] =
      request match {
        case Poll()                    => poll
        case Subscribe(topics)         => subscribe(topics)
        case Fetch(deferred)           => fetch(deferred)
        case Commit(offsets, deferred) => commit(offsets, deferred)
      }
  }

  private[this] def createConsumer[F[_], K, V](
    settings: ConsumerSettings[K, V]
  )(
    implicit F: Sync[F],
    context: ContextShift[F]
  ): Stream[F, Consumer[K, V]] =
    Stream.bracket {
      F.delay {
        new KConsumer(
          settings.nativeSettings.asJava,
          settings.keyDeserializer,
          settings.valueDeserializer
        )
      }
    } { consumer =>
      context.evalOn(settings.executionContext) {
        F.delay(consumer.close(settings.closeTimeout.asJava))
      }
    }

  private[kafka] def consumerStream[F[_], K, V](settings: ConsumerSettings[K, V])(
    implicit F: ConcurrentEffect[F],
    context: ContextShift[F],
    timer: Timer[F]
  ): Stream[F, KafkaConsumer[F, K, V]] =
    for {
      requests <- Stream.eval(Queue.unbounded[F, Request[F, K, V]])
      ref <- Stream.eval(Ref.of[F, State[F, K, V]](State.empty))
      consumer <- createConsumer(settings)
      actor = ConsumerActor(settings, ref, requests, consumer)
      handler <- Stream.bracket {
        requests.dequeue1
          .flatMap(actor.handle(_) *> context.shift)
          .foreverM[Unit]
          .start
      }(_.cancel)
      polls <- Stream.bracket {
        {
          requests.enqueue1(Poll()) *>
            timer.sleep(settings.pollInterval)
        }.foreverM[Unit].start
      }(_.cancel)
    } yield {
      new KafkaConsumer[F, K, V] {
        override def stream(sink: Sink[F, CommittableMessage[F, K, V]]): Stream[F, Unit] =
          Stream.eval(fiber.join).concurrently {
            Stream
              .repeatEval {
                Deferred[F, Stream[F, CommittableMessage[F, K, V]]]
                  .flatMap { deferred =>
                    requests.enqueue1(Fetch(deferred)) *> deferred.get
                  }
              }
              .flatten
              .to(sink)
          }

        override def subscribe(topics: NonEmptyList[String]): Stream[F, Unit] =
          Stream.eval(requests.enqueue1(Subscribe(topics)))

        override val fiber: Fiber[F, Unit] =
          handler combine polls
      }
    }
}

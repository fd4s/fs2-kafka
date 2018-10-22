package fs2.kafka

import java.util

import cats.data.{Chain, NonEmptyChain, NonEmptyList}
import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.syntax.concurrent._
import cats.effect.{ConcurrentEffect, Fiber, Timer, _}
import cats.instances.list._
import cats.instances.map._
import cats.instances.unit._
import cats.syntax.applicativeError._
import cats.syntax.apply._
import cats.syntax.flatMap._
import cats.syntax.foldable._
import cats.syntax.functor._
import cats.syntax.monadError._
import cats.syntax.semigroup._
import cats.syntax.traverse._
import fs2.{Chunk, Stream}
import fs2.concurrent.Queue
import fs2.kafka.internal.syntax._
import org.apache.kafka.clients.consumer.{KafkaConsumer => KConsumer, _}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._

sealed abstract class KafkaConsumer[F[_], K, V] {
  def stream: Stream[F, CommittableMessage[F, K, V]]

  def subscribe(topics: NonEmptyList[String]): Stream[F, Unit]

  def fiber: Fiber[F, Unit]
}

object KafkaConsumer {
  private[this] final class ExpiringFetch[F[_], K, V](
    deferred: Deferred[F, Chunk[CommittableMessage[F, K, V]]],
    expiresAt: Long
  ) {
    def complete(stream: Chunk[CommittableMessage[F, K, V]]): F[Unit] =
      deferred.complete(stream)

    def hasExpired(now: Long): Boolean =
      now >= expiresAt
  }

  private[this] final case class State[F[_], K, V](
    fetches: Map[TopicPartition, NonEmptyChain[ExpiringFetch[F, K, V]]],
    records: Map[TopicPartition, NonEmptyChain[CommittableMessage[F, K, V]]],
    subscribed: Boolean
  ) {
    def withFetch(
      partition: TopicPartition,
      deferred: Deferred[F, Chunk[CommittableMessage[F, K, V]]],
      expiresAt: Long
    ): State[F, K, V] = {
      val fetch = NonEmptyChain.one(new ExpiringFetch(deferred, expiresAt))
      copy(fetches = fetches combine Map(partition -> fetch))
    }

    def withoutFetches(partitions: Set[TopicPartition]): State[F, K, V] =
      copy(fetches = fetches.filterKeys(!partitions.contains(_)))

    def withRecords(
      records: Map[TopicPartition, NonEmptyChain[CommittableMessage[F, K, V]]]
    ): State[F, K, V] =
      copy(records = this.records combine records)

    def withoutRecords(partitions: Set[TopicPartition]): State[F, K, V] =
      copy(records = records.filterKeys(!partitions.contains(_)))

    def asSubscribed: State[F, K, V] =
      copy(subscribed = true)
  }

  private[this] object State {
    def empty[F[_], K, V]: State[F, K, V] =
      State(
        fetches = Map.empty,
        records = Map.empty,
        subscribed = false
      )
  }

  private[this] sealed abstract class Request[F[_], K, V]

  private[this] final case class Assignment[F[_], K, V](
    deferred: Deferred[F, Set[TopicPartition]]
  ) extends Request[F, K, V]

  private[this] final case class Revoked[F[_], K, V](
    partitions: Set[TopicPartition]
  ) extends Request[F, K, V]

  private[this] final case class Poll[F[_], K, V]() extends Request[F, K, V]

  private[this] final case class Subscribe[F[_], K, V](topics: NonEmptyList[String])
      extends Request[F, K, V]

  private[this] final case class Fetch[F[_], K, V](
    partition: TopicPartition,
    deferred: Deferred[F, Chunk[CommittableMessage[F, K, V]]]
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
        F.delay(
          consumer.subscribe(
            topics.toList.asJava,
            new ConsumerRebalanceListener {
              override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit =
                if (partitions.isEmpty) ()
                else {
                  val revoked = requests.enqueue1(Revoked(partitions.asScala.toSet))
                  F.runAsync(revoked)(_ => IO.unit).unsafeRunSync
                }

              override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit =
                ()
            }
          ))
      } *> ref.update(_.asSubscribed)

    private val nowExpiryTime: F[Long] =
      timer.clock.monotonic(settings.fetchTimeout.unit)

    private def fetch(
      partition: TopicPartition,
      deferred: Deferred[F, Chunk[CommittableMessage[F, K, V]]]
    ): F[Unit] = nowExpiryTime.flatMap { now =>
      val expiresAt = now + settings.fetchTimeout.length
      ref.update(_.withFetch(partition, deferred, expiresAt))
    }

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
                F.runAsync(complete)(_ => IO.unit).unsafeRunSync
              }
            }
          )
        }
      }

    private def revoked(partitions: Set[TopicPartition]): F[Unit] =
      ref.get.flatMap { state =>
        val fetches = state.fetches.keySet
        val records = state.records.keySet

        val withRecords = (partitions intersect fetches) intersect records
        val withoutRecords = (partitions intersect fetches) diff records

        val completeWithRecords =
          if (withRecords.nonEmpty) {
            state.fetches.filterKeys(withRecords).toList.traverse {
              case (partition, partitionFetches) =>
                val records = Chunk.chain(state.records(partition).toChain)
                partitionFetches.traverse(_.complete(records))
            } >> ref.update(_.withoutFetches(withRecords).withoutRecords(withRecords))
          } else F.unit

        val completeWithoutRecords =
          if (withoutRecords.nonEmpty) {
            state.fetches
              .filterKeys(withoutRecords)
              .values
              .toList
              .traverse(_.traverse(_.complete(Chunk.empty))) >>
              ref.update(_.withoutFetches(withoutRecords))
          } else F.unit

        completeWithRecords >> completeWithoutRecords
      }

    private def assignment(deferred: Deferred[F, Set[TopicPartition]]): F[Unit] =
      ref.get.flatMap { state =>
        val assigned =
          if (state.subscribed) {
            context.evalOn(settings.executionContext) {
              F.delay(consumer.assignment.asScala.toSet)
            }
          } else F.pure(Set.empty[TopicPartition])

        assigned.flatMap(deferred.complete)
      }

    private val messageCommit: Map[TopicPartition, OffsetAndMetadata] => F[Unit] =
      offsets =>
        Deferred[F, Either[Throwable, Unit]].flatMap { deferred =>
          requests.enqueue1(Commit(offsets, deferred)) >>
            F.race(timer.sleep(settings.commitTimeout), deferred.get.rethrow)
              .flatMap {
                case Right(_) => F.unit
                case Left(_) =>
                  F.raiseError[Unit] {
                    CommitTimeoutException(
                      settings.commitTimeout,
                      offsets
                    )
                  }
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
          commit = messageCommit
        )
      )

    private def records(
      batch: ConsumerRecords[K, V]
    ): Map[TopicPartition, NonEmptyChain[CommittableMessage[F, K, V]]] =
      if (batch.isEmpty) Map.empty
      else {
        val messages = Map.newBuilder[TopicPartition, NonEmptyChain[CommittableMessage[F, K, V]]]
        val partitions = batch.partitions.iterator

        while (partitions.hasNext) {
          val partition = partitions.next
          val records = batch.records(partition).iterator
          val partitionMessages = List.newBuilder[CommittableMessage[F, K, V]]

          while (records.hasNext) {
            val partitionMessage = message(records.next, partition)
            partitionMessages += partitionMessage
          }

          val partitionMessagesResult = Chain.fromSeq(partitionMessages.result)
          messages += partition -> NonEmptyChain.fromChainUnsafe(partitionMessagesResult)
        }

        messages.result
      }

    private val poll: F[Unit] = {
      ref.get.flatMap { state =>
        if (state.subscribed) {
          context
            .evalOn(settings.executionContext) {
              F.delay {
                val assigned = consumer.assignment.asScala.toSet
                val requested = state.fetches.keySet
                val available = state.records.keySet

                val resume = (assigned intersect requested) diff available
                val pause = assigned diff resume

                consumer.pause(pause.asJava)
                consumer.resume(resume.asJava)
                consumer.poll(settings.pollTimeout.asJava)
              }
            }
            .flatMap { batch =>
              if (state.fetches.isEmpty) {
                if (batch.isEmpty) F.unit
                else ref.update(_.withRecords(records(batch)))
              } else {
                val newRecords = records(batch)
                val allRecords = state.records combine newRecords

                if (allRecords.nonEmpty) {
                  val requested = state.fetches.keySet
                  val available = allRecords.keySet

                  val canBeCompleted = available intersect requested
                  val canBeStored = available diff canBeCompleted

                  val complete =
                    if (canBeCompleted.nonEmpty) {
                      state.fetches.filterKeys(canBeCompleted).toList.traverse {
                        case (partition, fetches) =>
                          val records = Chunk.chain(allRecords(partition).toChain)
                          fetches.traverse(_.complete(records))
                      } >> ref.update {
                        _.withoutFetches(canBeCompleted)
                          .withoutRecords(canBeCompleted)
                      }
                    } else F.unit

                  val store =
                    if (canBeStored.nonEmpty) {
                      ref.update(_.withRecords(newRecords.filterKeys(canBeStored)))
                    } else F.unit

                  complete >> store
                } else F.unit
              }
            }
            .flatMap { _ =>
              ref.get.flatMap { state =>
                nowExpiryTime.flatMap { now =>
                  val anyExpired =
                    state.fetches.values.exists(_.exists(_.hasExpired(now)))

                  if (anyExpired) {
                    val completeExpired =
                      state.fetches.values
                        .flatMap(_.filter_(_.hasExpired(now)))
                        .foldLeft(F.unit)(_ >> _.complete(Chunk.empty))

                    val newFetches =
                      Map.newBuilder[TopicPartition, NonEmptyChain[ExpiringFetch[F, K, V]]]

                    state.fetches.foreach {
                      case (partition, fetches) =>
                        if (!fetches.forall(_.hasExpired(now))) {
                          val nonExpiredFetches = fetches.filterNot(_.hasExpired(now))
                          newFetches += partition -> NonEmptyChain.fromChainUnsafe(nonExpiredFetches)
                        }
                    }

                    val removeExpired =
                      ref.update(_.copy(fetches = newFetches.result))

                    completeExpired >> removeExpired
                  } else F.unit
                }
              }
            }
        } else F.unit
      }
    }

    def handle(request: Request[F, K, V]): F[Unit] =
      request match {
        case Assignment(deferred)       => assignment(deferred)
        case Poll()                     => poll
        case Subscribe(topics)          => subscribe(topics)
        case Fetch(partition, deferred) => fetch(partition, deferred)
        case Commit(offsets, deferred)  => commit(offsets, deferred)
        case Revoked(partitions)        => revoked(partitions)
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
      polls <- Stream.eval(Queue.bounded[F, Request[F, K, V]](1))
      ref <- Stream.eval(Ref.of[F, State[F, K, V]](State.empty))
      consumer <- createConsumer(settings)
      actor = ConsumerActor(settings, ref, requests, consumer)
      handler <- Stream.bracket {
        requests.tryDequeue1
          .flatMap(_.map(F.pure).getOrElse(polls.dequeue1))
          .flatMap(actor.handle(_) >> context.shift)
          .foreverM[Unit]
          .start
      }(_.cancel)
      polls <- Stream.bracket {
        {
          polls.enqueue1(Poll()) >>
            timer.sleep(settings.pollInterval)
        }.foreverM[Unit].start
      }(_.cancel)
    } yield {
      new KafkaConsumer[F, K, V] {
        override def stream: Stream[F, CommittableMessage[F, K, V]] =
          Stream
            .repeatEval {
              val assignment: F[Set[TopicPartition]] =
                Deferred[F, Set[TopicPartition]]
                  .flatMap { deferred =>
                    val assignment = requests.enqueue1(Assignment(deferred)) >> deferred.get
                    F.race(fiber.join, assignment).map {
                      case Left(())        => Set.empty
                      case Right(assigned) => assigned
                    }
                  }

              assignment.flatMap { assigned =>
                if (assigned.isEmpty)
                  F.pure(Stream.empty.covaryAll[F, CommittableMessage[F, K, V]])
                else {
                  Queue
                    .bounded[F, Option[Chunk[CommittableMessage[F, K, V]]]](assigned.size)
                    .flatMap { queue =>
                      assigned.toList
                        .traverse { partition =>
                          Deferred[F, Chunk[CommittableMessage[F, K, V]]].flatMap { deferred =>
                            val fetch = requests.enqueue1(Fetch(partition, deferred)) >> deferred.get
                            F.race(fiber.join, fetch).flatMap {
                              case Left(())     => F.unit
                              case Right(chunk) => queue.enqueue1(Some(chunk))
                            }
                          }.start
                        }
                        .map(_.combineAll)
                        .flatMap(_.join)
                        .flatMap(_ => queue.enqueue1(None))
                        .start
                        .as {
                          queue.dequeue.unNoneTerminate
                            .flatMap(Stream.chunk)
                            .covary[F]
                        }
                    }
                }
              }
            }
            .flatten
            .interruptWhen(fiber.join.attempt)

        override def subscribe(topics: NonEmptyList[String]): Stream[F, Unit] =
          Stream.eval(requests.enqueue1(Subscribe(topics)))

        override val fiber: Fiber[F, Unit] =
          handler combine polls

        override def toString: String =
          "KafkaConsumer$" + System.identityHashCode(this)
      }
    }
}

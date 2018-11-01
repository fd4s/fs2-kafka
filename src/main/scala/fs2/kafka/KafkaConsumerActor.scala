/*
 * Copyright 2018 OVO Energy Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fs2.kafka

import java.util

import cats.data.{Chain, NonEmptyChain, NonEmptyList}
import cats.effect.concurrent.{Deferred, MVar, Ref}
import cats.effect.{ConcurrentEffect, ContextShift, IO, Timer}
import cats.instances.list._
import cats.instances.map._
import cats.syntax.flatMap._
import cats.syntax.foldable._
import cats.syntax.monadError._
import cats.syntax.semigroup._
import cats.syntax.traverse._
import fs2.Chunk
import fs2.concurrent.Queue
import fs2.kafka.KafkaConsumerActor.Request._
import fs2.kafka.KafkaConsumerActor._
import fs2.kafka.internal.syntax._
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._

private[kafka] final class KafkaConsumerActor[F[_], K, V](
  settings: ConsumerSettings[K, V],
  ref: Ref[F, State[F, K, V]],
  requests: Queue[F, Request[F, K, V]],
  mVar: MVar[F, Consumer[K, V]]
)(
  implicit F: ConcurrentEffect[F],
  context: ContextShift[F],
  timer: Timer[F]
) {
  private def withConsumer[A](f: Consumer[K, V] => F[A]): F[A] =
    mVar.lease { consumer =>
      context.evalOn(settings.executionContext) {
        f(consumer)
      }
    }

  private def subscribe(topics: NonEmptyList[String]): F[Unit] =
    withConsumer { consumer =>
      F.delay {
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
        )
      }
    } >> ref.update(_.asSubscribed)

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
    withConsumer { consumer =>
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

      val revokedFetches = partitions intersect fetches
      val withRecords = revokedFetches intersect records
      val withoutRecords = revokedFetches diff records

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

  private def assignment(deferred: Deferred[F, Either[Throwable, Set[TopicPartition]]]): F[Unit] =
    ref.get.flatMap { state =>
      val assigned: F[Either[Throwable, Set[TopicPartition]]] =
        if (state.subscribed) withConsumer { consumer =>
          F.delay(Right(consumer.assignment.asScala.toSet))
        } else F.pure(Left(NotSubscribedException))

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
    def pollConsumer(state: State[F, K, V]): F[ConsumerRecords[K, V]] =
      withConsumer { consumer =>
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

    def handleBatch(state: State[F, K, V], batch: ConsumerRecords[K, V]): F[Unit] =
      if (state.fetches.isEmpty) {
        if (batch.isEmpty) F.unit
        else ref.update(_.withRecords(records(batch)))
      } else {
        val newRecords = records(batch)
        val allRecords = state.records combine newRecords

        if (allRecords.nonEmpty) {
          val requested = state.fetches.keySet

          val canBeCompleted = allRecords.keySet intersect requested
          val canBeStored = newRecords.keySet diff canBeCompleted

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

    val completeExpiredFetches: F[Unit] =
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

    ref.get.flatMap { state =>
      if (state.subscribed) {
        pollConsumer(state).flatMap(handleBatch(state, _)) >> completeExpiredFetches
      } else F.unit
    }
  }

  private val shutdown: F[Unit] =
    ref.update(_.asShutdown)

  def handle(request: Request[F, K, V]): F[Unit] =
    request match {
      case Assignment(deferred)       => assignment(deferred)
      case Poll()                     => poll
      case Subscribe(topics)          => subscribe(topics)
      case Fetch(partition, deferred) => fetch(partition, deferred)
      case Commit(offsets, deferred)  => commit(offsets, deferred)
      case Revoked(partitions)        => revoked(partitions)
      case Shutdown()                 => shutdown
    }
}

private[kafka] object KafkaConsumerActor {
  final class ExpiringFetch[F[_], K, V](
    deferred: Deferred[F, Chunk[CommittableMessage[F, K, V]]],
    expiresAt: Long
  ) {
    def complete(chunk: Chunk[CommittableMessage[F, K, V]]): F[Unit] =
      deferred.complete(chunk)

    def hasExpired(now: Long): Boolean =
      now >= expiresAt
  }

  final case class State[F[_], K, V](
    fetches: Map[TopicPartition, NonEmptyChain[ExpiringFetch[F, K, V]]],
    records: Map[TopicPartition, NonEmptyChain[CommittableMessage[F, K, V]]],
    subscribed: Boolean,
    running: Boolean
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

    def asShutdown: State[F, K, V] =
      copy(running = false)
  }

  object State {
    def empty[F[_], K, V]: State[F, K, V] =
      State(
        fetches = Map.empty,
        records = Map.empty,
        subscribed = false,
        running = true
      )
  }

  sealed abstract class Request[F[_], K, V]

  object Request {
    final case class Assignment[F[_], K, V](
      deferred: Deferred[F, Either[Throwable, Set[TopicPartition]]]
    ) extends Request[F, K, V]

    final case class Revoked[F[_], K, V](
      partitions: Set[TopicPartition]
    ) extends Request[F, K, V]

    final case class Poll[F[_], K, V]() extends Request[F, K, V]

    final case class Subscribe[F[_], K, V](topics: NonEmptyList[String]) extends Request[F, K, V]

    final case class Fetch[F[_], K, V](
      partition: TopicPartition,
      deferred: Deferred[F, Chunk[CommittableMessage[F, K, V]]]
    ) extends Request[F, K, V]

    final case class Commit[F[_], K, V](
      offsets: Map[TopicPartition, OffsetAndMetadata],
      deferred: Deferred[F, Either[Throwable, Unit]]
    ) extends Request[F, K, V]

    final case class Shutdown[F[_], K, V]() extends Request[F, K, V]
  }
}

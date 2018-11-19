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

import java.time.Duration
import java.util

import cats.data.{Chain, NonEmptyChain, NonEmptyList, NonEmptySet}
import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{ConcurrentEffect, ContextShift, IO, Timer}
import cats.instances.list._
import cats.instances.map._
import cats.syntax.applicativeError._
import cats.syntax.flatMap._
import cats.syntax.foldable._
import cats.syntax.monadError._
import cats.syntax.semigroup._
import cats.syntax.traverse._
import fs2.Chunk
import fs2.concurrent.Queue
import fs2.kafka.KafkaConsumerActor._
import fs2.kafka.internal.Synchronized
import fs2.kafka.internal.instances._
import fs2.kafka.internal.syntax._
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.collection.immutable.SortedSet

private[kafka] final class KafkaConsumerActor[F[_], K, V](
  settings: ConsumerSettings[K, V],
  ref: Ref[F, State[F, K, V]],
  requests: Queue[F, Request[F, K, V]],
  synchronized: Synchronized[F, Consumer[K, V]]
)(
  implicit F: ConcurrentEffect[F],
  context: ContextShift[F],
  jitter: Jitter[F],
  timer: Timer[F]
) {
  private def withConsumer[A](f: Consumer[K, V] => F[A]): F[A] =
    synchronized.use { consumer =>
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
                val nonEmpty = NonEmptySet.fromSetUnsafe(partitions.toSortedSet)
                val revoked = requests.enqueue1(Request.Revoked(nonEmpty))
                F.runAsync(revoked)(_ => IO.unit).unsafeRunSync
              }

            override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit =
              if (partitions.isEmpty) ()
              else {
                val nonEmpty = NonEmptySet.fromSetUnsafe(partitions.toSortedSet)
                val assigned = requests.enqueue1(Request.Assigned(nonEmpty))
                F.runAsync(assigned)(_ => IO.unit).unsafeRunSync
              }
          }
        )
      }
    } >> ref.update(_.asSubscribed)

  private val nowExpiryTime: F[Long] =
    timer.clock.monotonic(settings.fetchTimeout.unit)

  private def expiringFetch(
    partition: TopicPartition,
    deferred: Deferred[F, (Chunk[CommittableMessage[F, K, V]], ExpiringFetchCompletedReason)]
  ): F[Unit] = {
    val assigned =
      withConsumer { consumer =>
        F.delay(consumer.assignment.contains(partition))
      }

    def storeFetch =
      nowExpiryTime.flatMap { now =>
        val expiresAt = now + settings.fetchTimeout.length
        ref.update(_.withExpiringFetch(partition, deferred, expiresAt))
      }

    def completeRevoked =
      deferred.complete((Chunk.empty, ExpiringFetchCompletedReason.TopicPartitionRevoked))

    assigned.ifM(storeFetch, completeRevoked)
  }

  private def fetch(
    partition: TopicPartition,
    deferred: Deferred[F, (Chunk[CommittableMessage[F, K, V]], FetchCompletedReason)]
  ): F[Unit] = {
    val assigned =
      withConsumer { consumer =>
        F.delay(consumer.assignment.contains(partition))
      }

    def storeFetch =
      ref.update(_.withFetch(partition, deferred))

    def completeRevoked =
      deferred.complete((Chunk.empty, FetchCompletedReason.TopicPartitionRevoked))

    assigned.ifM(storeFetch, completeRevoked)
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

  private def assigned(assigned: Request.Assigned[F, K, V]): F[Unit] =
    ref.get.flatMap(_.onRebalances.foldLeft(F.unit)(_ >> _.onAssigned(assigned)))

  private def revoked(revoked: Request.Revoked[F, K, V]): F[Unit] =
    ref.get.flatMap { state =>
      val fetches = state.fetches.keySetStrict
      val records = state.records.keySetStrict

      val revokedFetches = revoked.partitions.toSortedSet intersect fetches
      val withRecords = records intersect revokedFetches
      val withoutRecords = revokedFetches diff records

      val completeWithRecords =
        if (withRecords.nonEmpty) {
          state.fetches.filterKeysStrictList(withRecords).traverse {
            case (partition, partitionFetches) =>
              val records = Chunk.chain(state.records(partition).toChain)
              partitionFetches.traverse(_.completeRevoked(records))
          } >> ref.update(_.withoutFetchesAndRecords(withRecords))
        } else F.unit

      val completeWithoutRecords =
        if (withoutRecords.nonEmpty) {
          state.fetches
            .filterKeysStrictValuesList(withoutRecords)
            .traverse(_.traverse(_.completeRevoked(Chunk.empty))) >>
            ref.update(_.withoutFetches(withoutRecords))
        } else F.unit

      val onRevoked =
        state.onRebalances.foldLeft(F.unit)(_ >> _.onRevoked(revoked))

      completeWithRecords >> completeWithoutRecords >> onRevoked
    }

  private def assignment(
    deferred: Deferred[F, Either[Throwable, SortedSet[TopicPartition]]],
    onRebalance: Option[OnRebalance[F, K, V]]
  ): F[Unit] =
    ref.get.flatMap { state =>
      val assigned: F[Either[Throwable, SortedSet[TopicPartition]]] =
        if (state.subscribed) withConsumer { consumer =>
          F.delay(Right(consumer.assignment.toSortedSet))
        } else F.pure(Left(NotSubscribedException))

      val withOnRebalance =
        onRebalance.fold(F.unit)(on => ref.update(_.withOnRebalance(on)))

      assigned.flatMap(deferred.complete) >> withOnRebalance
    }

  private val messageCommit: Map[TopicPartition, OffsetAndMetadata] => F[Unit] =
    offsets => {
      val commit =
        Deferred[F, Either[Throwable, Unit]].flatMap { deferred =>
          requests.enqueue1(Request.Commit(offsets, deferred)) >>
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

      commit.handleErrorWith {
        settings.commitRecovery
          .recoverCommitWith(offsets, commit)
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

  private val pollTimeout: Duration =
    settings.pollTimeout.asJava

  private val poll: F[Unit] = {
    def pollConsumer(state: State[F, K, V]): F[ConsumerRecords[K, V]] =
      withConsumer { consumer =>
        F.delay {
          val assigned = consumer.assignment.toSet
          val requested = state.fetches.keySetStrict
          val available = state.records.keySetStrict

          val resume = (requested intersect assigned) diff available
          val pause = assigned diff resume

          consumer.pause(pause.asJava)
          consumer.resume(resume.asJava)
          consumer.poll(pollTimeout)
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
          val requested = state.fetches.keySetStrict

          val canBeCompleted = allRecords.keySetStrict intersect requested
          val canBeStored = newRecords.keySetStrict diff canBeCompleted

          val complete =
            if (canBeCompleted.nonEmpty) {
              state.fetches.filterKeysStrictList(canBeCompleted).traverse {
                case (partition, fetches) =>
                  val records = Chunk.chain(allRecords(partition).toChain)
                  fetches.traverse(_.completeRecords(records))
              } >> ref.update(_.withoutFetchesAndRecords(canBeCompleted))
            } else F.unit

          val store =
            if (canBeStored.nonEmpty) {
              ref.update(_.withRecords(newRecords.filterKeysStrict(canBeStored)))
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
                .foldLeft(F.unit)(_ >> _.foldLeft(F.unit) {
                  case (es, e @ ExpiringFetch(_, _)) if e.hasExpired(now) =>
                    es >> e.completeExpired
                  case (es, _) => es
                })

            val newFetches =
              Map.newBuilder[TopicPartition, NonEmptyChain[FetchRequest[F, K, V]]]

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
        pollConsumer(state)
          .flatMap(handleBatch(state, _))
          .flatMap(_ => completeExpiredFetches)
      } else F.unit
    }
  }

  def handle(request: Request[F, K, V]): F[Unit] =
    request match {
      case Request.Assignment(deferred, onRebalance)  => assignment(deferred, onRebalance)
      case request @ Request.Assigned(_)              => assigned(request)
      case Request.Poll()                             => poll
      case Request.Subscribe(topics)                  => subscribe(topics)
      case Request.Fetch(partition, deferred)         => fetch(partition, deferred)
      case Request.ExpiringFetch(partition, deferred) => expiringFetch(partition, deferred)
      case Request.Commit(offsets, deferred)          => commit(offsets, deferred)
      case request @ Request.Revoked(_)               => revoked(request)
    }
}

private[kafka] object KafkaConsumerActor {
  sealed abstract class FetchRequest[F[_], K, V] {
    def completeRevoked(chunk: Chunk[CommittableMessage[F, K, V]]): F[Unit]

    def completeRecords(chunk: Chunk[CommittableMessage[F, K, V]]): F[Unit]

    def hasExpired(now: Long): Boolean
  }

  final case class ExpiringFetch[F[_], K, V](
    deferred: Deferred[F, (Chunk[CommittableMessage[F, K, V]], ExpiringFetchCompletedReason)],
    expiresAt: Long
  ) extends FetchRequest[F, K, V] {
    override def completeRevoked(chunk: Chunk[CommittableMessage[F, K, V]]): F[Unit] =
      deferred.complete((chunk, ExpiringFetchCompletedReason.TopicPartitionRevoked))

    override def completeRecords(chunk: Chunk[CommittableMessage[F, K, V]]): F[Unit] =
      deferred.complete((chunk, ExpiringFetchCompletedReason.FetchedRecords))

    val completeExpired: F[Unit] =
      deferred.complete((Chunk.empty, ExpiringFetchCompletedReason.FetchExpired))

    override def hasExpired(now: Long): Boolean =
      now >= expiresAt
  }

  final case class NonExpiringFetch[F[_], K, V](
    deferred: Deferred[F, (Chunk[CommittableMessage[F, K, V]], FetchCompletedReason)]
  ) extends FetchRequest[F, K, V] {
    override def completeRevoked(chunk: Chunk[CommittableMessage[F, K, V]]): F[Unit] =
      deferred.complete((chunk, FetchCompletedReason.TopicPartitionRevoked))

    override def completeRecords(chunk: Chunk[CommittableMessage[F, K, V]]): F[Unit] =
      deferred.complete((chunk, FetchCompletedReason.FetchedRecords))

    override def hasExpired(now: Long): Boolean =
      false
  }

  final case class State[F[_], K, V](
    fetches: Map[TopicPartition, NonEmptyChain[FetchRequest[F, K, V]]],
    records: Map[TopicPartition, NonEmptyChain[CommittableMessage[F, K, V]]],
    onRebalances: Chain[OnRebalance[F, K, V]],
    subscribed: Boolean
  ) {
    def withOnRebalance(onRebalance: OnRebalance[F, K, V]): State[F, K, V] =
      copy(onRebalances = onRebalances append onRebalance)

    def withFetch(
      partition: TopicPartition,
      deferred: Deferred[F, (Chunk[CommittableMessage[F, K, V]], FetchCompletedReason)]
    ): State[F, K, V] = {
      val fetch = NonEmptyChain.one(NonExpiringFetch(deferred))
      copy(fetches = fetches combine Map(partition -> fetch))
    }

    def withExpiringFetch(
      partition: TopicPartition,
      deferred: Deferred[F, (Chunk[CommittableMessage[F, K, V]], ExpiringFetchCompletedReason)],
      expiresAt: Long
    ): State[F, K, V] = {
      val fetch = NonEmptyChain.one(ExpiringFetch(deferred, expiresAt))
      copy(fetches = fetches combine Map(partition -> fetch))
    }

    def withoutFetches(partitions: Set[TopicPartition]): State[F, K, V] =
      copy(fetches = fetches.filterKeysStrict(!partitions.contains(_)))

    def withRecords(
      records: Map[TopicPartition, NonEmptyChain[CommittableMessage[F, K, V]]]
    ): State[F, K, V] =
      copy(records = this.records combine records)

    def withoutFetchesAndRecords(partitions: Set[TopicPartition]): State[F, K, V] =
      copy(
        fetches = fetches.filterKeysStrict(!partitions.contains(_)),
        records = records.filterKeysStrict(!partitions.contains(_))
      )

    def asSubscribed: State[F, K, V] =
      if (subscribed) this else copy(subscribed = true)
  }

  object State {
    def empty[F[_], K, V]: State[F, K, V] =
      State(
        fetches = Map.empty,
        records = Map.empty,
        onRebalances = Chain.empty,
        subscribed = false
      )
  }

  sealed abstract class FetchCompletedReason {
    final def topicPartitionRevoked: Boolean = this match {
      case FetchCompletedReason.TopicPartitionRevoked => true
      case FetchCompletedReason.FetchedRecords        => false
    }
  }

  object FetchCompletedReason {
    case object TopicPartitionRevoked extends FetchCompletedReason

    case object FetchedRecords extends FetchCompletedReason
  }

  sealed abstract class ExpiringFetchCompletedReason

  object ExpiringFetchCompletedReason {
    case object TopicPartitionRevoked extends ExpiringFetchCompletedReason

    case object FetchedRecords extends ExpiringFetchCompletedReason

    case object FetchExpired extends ExpiringFetchCompletedReason
  }

  final case class OnRebalance[F[_], K, V](
    onAssigned: Request.Assigned[F, K, V] => F[Unit],
    onRevoked: Request.Revoked[F, K, V] => F[Unit]
  )

  sealed abstract class Request[F[_], K, V]

  object Request {
    final case class Assignment[F[_], K, V](
      deferred: Deferred[F, Either[Throwable, SortedSet[TopicPartition]]],
      onRebalance: Option[OnRebalance[F, K, V]]
    ) extends Request[F, K, V]

    final case class Assigned[F[_], K, V](
      partitions: NonEmptySet[TopicPartition]
    ) extends Request[F, K, V]

    final case class Revoked[F[_], K, V](
      partitions: NonEmptySet[TopicPartition]
    ) extends Request[F, K, V]

    final case class Poll[F[_], K, V]() extends Request[F, K, V]

    final case class Subscribe[F[_], K, V](topics: NonEmptyList[String]) extends Request[F, K, V]

    final case class Fetch[F[_], K, V](
      partition: TopicPartition,
      deferred: Deferred[F, (Chunk[CommittableMessage[F, K, V]], FetchCompletedReason)]
    ) extends Request[F, K, V]

    final case class ExpiringFetch[F[_], K, V](
      partition: TopicPartition,
      deferred: Deferred[F, (Chunk[CommittableMessage[F, K, V]], ExpiringFetchCompletedReason)]
    ) extends Request[F, K, V]

    final case class Commit[F[_], K, V](
      offsets: Map[TopicPartition, OffsetAndMetadata],
      deferred: Deferred[F, Either[Throwable, Unit]]
    ) extends Request[F, K, V]
  }
}

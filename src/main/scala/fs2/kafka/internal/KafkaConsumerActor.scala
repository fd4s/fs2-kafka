/*
 * Copyright 2018-2019 OVO Energy Limited
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

package fs2.kafka.internal

import java.time.Duration
import java.util
import java.util.regex.Pattern

import cats.data.{Chain, NonEmptyChain, NonEmptyList, NonEmptySet}
import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{ConcurrentEffect, ContextShift, IO, Timer}
import cats.instances.list._
import cats.instances.map._
import cats.syntax.applicativeError._
import cats.syntax.flatMap._
import cats.syntax.monadError._
import cats.syntax.semigroup._
import cats.syntax.traverse._
import fs2.Chunk
import fs2.concurrent.Queue
import fs2.kafka._
import fs2.kafka.internal.KafkaConsumerActor._
import fs2.kafka.internal.instances._
import fs2.kafka.internal.syntax._
import org.apache.kafka.clients.consumer.{ConsumerRecord => _, _}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.collection.immutable.SortedSet
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

/**
  * [[KafkaConsumerActor]] wraps a Java `KafkaConsumer` and works similar to
  * a traditional actor, in the sense that it receives requests one at-a-time
  * via a queue, which are received as calls to the `handle` function. `Poll`
  * requests are scheduled at a fixed interval and, when handled, calls the
  * `KafkaConsumer#poll` function, allowing the Java consumer to perform
  * necessary background functions, and to return fetched records.<br>
  * <br>
  * The actor receives `Fetch` requests for topic-partitions for which there
  * is demand. The actor then attempts to fetch records for topic-partitions
  * where there is a `Fetch` request. For topic-partitions where there is no
  * request, no attempt to fetch records is made. This effectively enables
  * backpressure, as long as `Fetch` requests are only issued when there
  * is more demand.
  */
private[kafka] final class KafkaConsumerActor[F[_], K, V](
  settings: ConsumerSettings[K, V],
  executionContext: ExecutionContext,
  ref: Ref[F, State[F, K, V]],
  requests: Queue[F, Request[F, K, V]],
  synchronized: Synchronized[F, Consumer[K, V]]
)(
  implicit F: ConcurrentEffect[F],
  context: ContextShift[F],
  jitter: Jitter[F],
  timer: Timer[F]
) {
  private[this] def withConsumer[A](f: Consumer[K, V] => F[A]): F[A] =
    synchronized.use { consumer =>
      context.evalOn(executionContext) {
        f(consumer)
      }
    }

  private[this] val consumerRebalanceListener: ConsumerRebalanceListener =
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

  private[this] def beginningOffsets(
    partitions: Set[TopicPartition],
    timeout: Option[FiniteDuration],
    deferred: Deferred[F, Either[Throwable, Map[TopicPartition, Long]]]
  ): F[Unit] = {
    val beginningOffsets =
      withConsumer { consumer =>
        F.delay {
          (timeout match {
            case None           => consumer.beginningOffsets(partitions.asJava)
            case Some(duration) => consumer.beginningOffsets(partitions.asJava, duration.asJava)
          }).asInstanceOf[util.Map[TopicPartition, Long]].toMap
        }.attempt
      }

    beginningOffsets.flatMap(deferred.complete)
  }

  private[this] def endOffsets(
    partitions: Set[TopicPartition],
    timeout: Option[FiniteDuration],
    deferred: Deferred[F, Either[Throwable, Map[TopicPartition, Long]]]
  ): F[Unit] = {
    val endOffsets =
      withConsumer { consumer =>
        F.delay {
          (timeout match {
            case None           => consumer.endOffsets(partitions.asJava)
            case Some(duration) => consumer.endOffsets(partitions.asJava, duration.asJava)
          }).asInstanceOf[util.Map[TopicPartition, Long]].toMap
        }.attempt
      }

    endOffsets.flatMap(deferred.complete)
  }

  private[this] def subscribe(
    topics: NonEmptyList[String],
    deferred: Deferred[F, Either[Throwable, Unit]]
  ): F[Unit] = {
    val subscribe =
      withConsumer { consumer =>
        F.delay {
          consumer.subscribe(
            topics.toList.asJava,
            consumerRebalanceListener
          )
        }.attempt
      }

    subscribe
      .flatTap {
        case Left(_)  => F.unit
        case Right(_) => ref.update(_.asSubscribed)
      }
      .flatMap(deferred.complete)
  }

  private[this] def subscribe(
    pattern: Pattern,
    deferred: Deferred[F, Either[Throwable, Unit]]
  ): F[Unit] = {
    val subscribe =
      withConsumer { consumer =>
        F.delay {
          consumer.subscribe(
            pattern,
            consumerRebalanceListener
          )
        }.attempt
      }

    subscribe
      .flatTap {
        case Left(_)  => F.unit
        case Right(_) => ref.update(_.asSubscribed)
      }
      .flatMap(deferred.complete)
  }

  private[this] def seek(
    partition: TopicPartition,
    offset: Long,
    deferred: Deferred[F, Either[Throwable, Unit]]
  ): F[Unit] = {
    val seek =
      withConsumer { consumer =>
        F.delay {
          consumer.seek(
            partition,
            offset
          )
        }.attempt
      }

    seek.flatMap(deferred.complete)
  }

  private[this] def fetch(
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

  private[this] def commit(
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

  private[this] def assigned(assigned: Request.Assigned[F, K, V]): F[Unit] =
    ref.get.flatMap(_.onRebalances.foldLeft(F.unit)(_ >> _.onAssigned(assigned)))

  private[this] def revoked(revoked: Request.Revoked[F, K, V]): F[Unit] =
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
              val records = Chunk.buffer(state.records(partition))
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

  private[this] def assignment(
    deferred: Deferred[F, Either[Throwable, SortedSet[TopicPartition]]],
    onRebalance: Option[OnRebalance[F, K, V]]
  ): F[Unit] =
    ref.get.flatMap { state =>
      val assigned: F[Either[Throwable, SortedSet[TopicPartition]]] =
        if (state.subscribed) withConsumer { consumer =>
          F.delay(Right(consumer.assignment.toSortedSet))
        } else F.pure(Left(NotSubscribedException()))

      val withOnRebalance =
        onRebalance.fold(F.unit)(on => ref.update(_.withOnRebalance(on)))

      assigned.flatMap(deferred.complete) >> withOnRebalance
    }

  private[this] val messageCommit: Map[TopicPartition, OffsetAndMetadata] => F[Unit] =
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

  private[this] def message(
    record: ConsumerRecord[K, V],
    partition: TopicPartition
  ): CommittableMessage[F, K, V] =
    CommittableMessage(
      record = record,
      committableOffset = CommittableOffset(
        topicPartition = partition,
        offsetAndMetadata = new OffsetAndMetadata(
          record.offset + 1L,
          settings.recordMetadata(record)
        ),
        commit = messageCommit
      )
    )

  private[this] def records(
    batch: ConsumerRecords[K, V]
  ): Map[TopicPartition, ArrayBuffer[CommittableMessage[F, K, V]]] =
    if (batch.isEmpty) Map.empty
    else {
      var messages = Map.empty[TopicPartition, ArrayBuffer[CommittableMessage[F, K, V]]]

      val partitions = batch.partitions.iterator
      while (partitions.hasNext) {
        val partition = partitions.next
        val records = batch.records(partition)
        val partitionMessages = new ArrayBuffer[CommittableMessage[F, K, V]](records.size)

        val it = records.iterator
        while (it.hasNext) {
          val next = ConsumerRecord.fromJava(it.next)
          partitionMessages += message(next, partition)
        }

        messages = messages.updated(partition, partitionMessages)
      }

      messages
    }

  private[this] val pollTimeout: Duration =
    settings.pollTimeout.asJava

  private[this] val poll: F[Unit] = {
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
                  val records = Chunk.buffer(allRecords(partition))
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

    ref.get.flatMap { state =>
      if (state.subscribed) {
        pollConsumer(state)
          .flatMap(handleBatch(state, _))
      } else F.unit
    }
  }

  def handle(request: Request[F, K, V]): F[Unit] =
    request match {
      case Request.Assignment(deferred, onRebalance) => assignment(deferred, onRebalance)
      case request @ Request.Assigned(_)             => assigned(request)
      case Request.BeginningOffsets(partitions, timeout, deferred) =>
        beginningOffsets(partitions, timeout, deferred)
      case Request.EndOffsets(partitions, timeout, deferred) =>
        endOffsets(partitions, timeout, deferred)
      case Request.Poll()                              => poll
      case Request.SubscribeTopics(topics, deferred)   => subscribe(topics, deferred)
      case Request.SubscribePattern(pattern, deferred) => subscribe(pattern, deferred)
      case Request.Fetch(partition, deferred)          => fetch(partition, deferred)
      case Request.Commit(offsets, deferred)           => commit(offsets, deferred)
      case request @ Request.Revoked(_)                => revoked(request)
      case Request.Seek(partition, offset, deferred)   => seek(partition, offset, deferred)
    }
}

private[kafka] object KafkaConsumerActor {
  final case class FetchRequest[F[_], K, V](
    deferred: Deferred[F, (Chunk[CommittableMessage[F, K, V]], FetchCompletedReason)]
  ) {
    def completeRevoked(chunk: Chunk[CommittableMessage[F, K, V]]): F[Unit] =
      deferred.complete((chunk, FetchCompletedReason.TopicPartitionRevoked))

    def completeRecords(chunk: Chunk[CommittableMessage[F, K, V]]): F[Unit] =
      deferred.complete((chunk, FetchCompletedReason.FetchedRecords))
  }

  final case class State[F[_], K, V](
    fetches: Map[TopicPartition, NonEmptyChain[FetchRequest[F, K, V]]],
    records: Map[TopicPartition, ArrayBuffer[CommittableMessage[F, K, V]]],
    onRebalances: Chain[OnRebalance[F, K, V]],
    subscribed: Boolean
  ) {
    def withOnRebalance(onRebalance: OnRebalance[F, K, V]): State[F, K, V] =
      copy(onRebalances = onRebalances append onRebalance)

    def withFetch(
      partition: TopicPartition,
      deferred: Deferred[F, (Chunk[CommittableMessage[F, K, V]], FetchCompletedReason)]
    ): State[F, K, V] = {
      val fetch = NonEmptyChain.one(FetchRequest(deferred))
      copy(fetches = fetches combine Map(partition -> fetch))
    }

    def withoutFetches(partitions: Set[TopicPartition]): State[F, K, V] =
      copy(fetches = fetches.filterKeysStrict(!partitions.contains(_)))

    def withRecords(
      records: Map[TopicPartition, ArrayBuffer[CommittableMessage[F, K, V]]]
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

    private[this] val pollInstance: Poll[Nothing, Nothing, Nothing] =
      Poll[Nothing, Nothing, Nothing]()

    def poll[F[_], K, V]: Poll[F, K, V] =
      pollInstance.asInstanceOf[Poll[F, K, V]]

    final case class SubscribeTopics[F[_], K, V](
      topics: NonEmptyList[String],
      deferred: Deferred[F, Either[Throwable, Unit]]
    ) extends Request[F, K, V]

    final case class SubscribePattern[F[_], K, V](
      pattern: Pattern,
      deferred: Deferred[F, Either[Throwable, Unit]]
    ) extends Request[F, K, V]

    final case class Fetch[F[_], K, V](
      partition: TopicPartition,
      deferred: Deferred[F, (Chunk[CommittableMessage[F, K, V]], FetchCompletedReason)]
    ) extends Request[F, K, V]

    final case class BeginningOffsets[F[_], K, V](
      partitions: Set[TopicPartition],
      timeout: Option[FiniteDuration],
      deferred: Deferred[F, Either[Throwable, Map[TopicPartition, Long]]]
    ) extends Request[F, K, V]

    final case class EndOffsets[F[_], K, V](
      partitions: Set[TopicPartition],
      timeout: Option[FiniteDuration],
      deferred: Deferred[F, Either[Throwable, Map[TopicPartition, Long]]]
    ) extends Request[F, K, V]

    final case class Commit[F[_], K, V](
      offsets: Map[TopicPartition, OffsetAndMetadata],
      deferred: Deferred[F, Either[Throwable, Unit]]
    ) extends Request[F, K, V]

    final case class Seek[F[_], K, V](
      partition: TopicPartition,
      offset: Long,
      deferred: Deferred[F, Either[Throwable, Unit]]
    ) extends Request[F, K, V]
  }
}

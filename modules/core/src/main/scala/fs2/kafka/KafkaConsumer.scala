/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.{Foldable, Functor, Reducible}
import cats.data.{NonEmptyList, NonEmptySet, OptionT}
import cats.effect._
import cats.effect.concurrent.TryableDeferred
import cats.effect.implicits._
import cats.implicits._
import fs2.{Chunk, Stream}
import fs2.concurrent.{NoneTerminatedQueue, Queue}
import fs2.kafka.internal._
import fs2.kafka.internal.converters.collection._
import fs2.kafka.instances._
import fs2.kafka.internal.KafkaConsumerActor._
import fs2.kafka.internal.syntax._
import fs2.kafka.consumer._

import java.util
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.{Metric, MetricName, PartitionInfo, TopicPartition}

import scala.collection.immutable.SortedSet
import scala.concurrent.duration.FiniteDuration
import scala.util.matching.Regex
import cats.effect.{ Deferred, Ref, Spawn, Temporal }

/**
  * [[KafkaConsumer]] represents a consumer of Kafka records, with the
  * ability to `subscribe` to topics, start a single top-level stream,
  * and optionally control it via the provided [[fiber]] instance.<br>
  * <br>
  * The following top-level streams are provided.<br>
  * <br>
  * - [[stream]] provides a single stream of records, where the order
  *   of records is guaranteed per topic-partition.<br>
  * - [[partitionedStream]] provides a stream with elements as streams
  *   that continually request records for a single partition. Order
  *   is guaranteed per topic-partition, but all assigned partitions
  *   will have to be processed in parallel.<br>
  * - [[partitionsMapStream]] provides a stream where each element contains
  *   a current assignment. The current assignment is the `Map`, where keys
  *   is a `TopicPartition`, and values are streams with records for a
  *   particular `TopicPartition`.
  * <br>
  * For the streams, records are wrapped in [[CommittableConsumerRecord]]s
  * which provide [[CommittableOffset]]s with the ability to commit
  * record offsets to Kafka. For performance reasons, offsets are
  * usually committed in batches using [[CommittableOffsetBatch]].
  * Provided `Pipe`s, like [[commitBatchWithin]] are available for
  * batch committing offsets. If you are not committing offsets to
  * Kafka, you can simply discard the [[CommittableOffset]], and
  * only make use of the record.<br>
  * <br>
  * While it's technically possible to start more than one stream from a
  * single [[KafkaConsumer]], it is generally not recommended as there is
  * no guarantee which stream will receive which records, and there might
  * be an overlap, in terms of duplicate records, between the two streams.
  * If a first stream completes, possibly with error, there's no guarantee
  * the stream has processed all of the records it received, and a second
  * stream from the same [[KafkaConsumer]] might not be able to pick up where
  * the first one left off. Therefore, only create a single top-level stream
  * per [[KafkaConsumer]], and if you want to start a new stream if the first
  * one finishes, let the [[KafkaConsumer]] shutdown and create a new one.
  */
sealed abstract class KafkaConsumer[F[_], K, V]
    extends KafkaConsume[F, K, V]
    with KafkaAssignment[F]
    with KafkaOffsets[F]
    with KafkaSubscription[F]
    with KafkaTopics[F]
    with KafkaCommit[F]
    with KafkaMetrics[F]
    with KafkaConsumerLifecycle[F]

object KafkaConsumer {
  private def spawnRepeating[F[_]: Concurrent, A](fa: F[A]): Resource[F, Fiber[F, Unit]] =
    Resource.make {
      Deferred[F, Either[Throwable, Unit]].flatMap { deferred =>
        fa.foreverM[Unit]
          .guaranteeCase {
            case ExitCase.Error(e) => deferred.complete(Left(e))
            case _                 => deferred.complete(Right(()))
          }
          .start
          .map(fiber => Fiber[F, Unit](deferred.get.rethrow, fiber.cancel.start.void))
      }
    }(_.cancel)

  private def startConsumerActor[F[_], K, V](
    requests: Queue[F, Request[F, K, V]],
    polls: Queue[F, Request[F, K, V]],
    actor: KafkaConsumerActor[F, K, V]
  )(
    implicit F: Concurrent[F]): Resource[F, Fiber[F, Unit]] =
    spawnRepeating {
      OptionT(requests.tryDequeue1)
        .getOrElseF(polls.dequeue1)
        .flatMap(actor.handle(_) >> Spawn[F].cede)
    }

  private def startPollScheduler[F[_], K, V](
    polls: Queue[F, Request[F, K, V]],
    pollInterval: FiniteDuration
  )(
    implicit F: Concurrent[F],
    timer: Temporal[F]
  ): Resource[F, Fiber[F, Unit]] =
    spawnRepeating {
      polls.enqueue1(Request.poll) >> timer.sleep(pollInterval)
    }

  private def createKafkaConsumer[F[_], K, V](
    requests: Queue[F, Request[F, K, V]],
    settings: ConsumerSettings[F, K, V],
    actor: Fiber[F, Unit],
    polls: Fiber[F, Unit],
    streamIdRef: Ref[F, StreamId],
    id: Int,
    withConsumer: WithConsumer[F],
    stopConsumingDeferred: TryableDeferred[F, Unit]
  )(implicit F: Concurrent[F]): KafkaConsumer[F, K, V] =
    new KafkaConsumer[F, K, V] {

      override val fiber: Fiber[F, Unit] = {
        val actorFiber =
          Fiber[F, Unit](actor.join.guaranteeCase {
            case ExitCase.Completed => polls.cancel
            case _                  => F.unit
          }, actor.cancel)

        val pollsFiber =
          Fiber[F, Unit](polls.join.guaranteeCase {
            case ExitCase.Completed => actor.cancel
            case _                  => F.unit
          }, polls.cancel)

        actorFiber combine pollsFiber
      }

      override def partitionsMapStream
        : Stream[F, Map[TopicPartition, Stream[F, CommittableConsumerRecord[F, K, V]]]] = {
        val chunkQueue: F[Queue[F, Option[Chunk[CommittableConsumerRecord[F, K, V]]]]] =
          Queue.bounded(settings.maxPrefetchBatches - 1)

        type PartitionRequest =
          (Chunk[CommittableConsumerRecord[F, K, V]], FetchCompletedReason)

        type PartitionsMap = Map[TopicPartition, Stream[F, CommittableConsumerRecord[F, K, V]]]
        type PartitionsMapQueue = NoneTerminatedQueue[F, PartitionsMap]

        def createPartitionStream(
          streamId: StreamId,
          partition: TopicPartition,
          assignmentRevoked: F[Unit]
        ): F[Stream[F, CommittableConsumerRecord[F, K, V]]] =
          for {
            chunks <- chunkQueue
            dequeueDone <- Deferred[F, Unit]
            shutdown = F
              .race(
                F.race(
                  awaitTermination.attempt,
                  dequeueDone.get
                ),
                F.race(
                  stopConsumingDeferred.get,
                  assignmentRevoked
                )
              )
              .void
            stopReqs <- Deferred.tryable[F, Unit]
          } yield Stream.eval {
            def fetchPartition(deferred: Deferred[F, PartitionRequest]): F[Unit] = {
              val request =
                Request.Fetch(partition, streamId, deferred.complete)
              val fetch = requests.enqueue1(request) >> deferred.get
              F.race(shutdown, fetch).flatMap {
                case Left(()) =>
                  stopReqs.complete(())

                case Right((chunk, reason)) =>
                  val enqueueChunk = chunks.enqueue1(Some(chunk)).unlessA(chunk.isEmpty)

                  val completeRevoked =
                    stopReqs.complete(()).whenA(reason.topicPartitionRevoked)

                  enqueueChunk >> completeRevoked
              }
            }

            Stream
              .repeatEval {
                stopReqs.tryGet.flatMap {
                  case None =>
                    Deferred[F, PartitionRequest] >>= fetchPartition

                  case Some(()) =>
                    // Prevent issuing additional requests after partition is
                    // revoked or shutdown happens, in case the stream isn't
                    // interrupted fast enough
                    F.unit
                }
              }
              .interruptWhen(F.race(shutdown, stopReqs.get).void.attempt)
              .compile
              .drain
              .guarantee(F.race(dequeueDone.get, chunks.enqueue1(None)).void)
              .start
              .as {
                chunks.dequeue.unNoneTerminate
                  .flatMap(Stream.chunk)
                  .covary[F]
                  .onFinalize(dequeueDone.complete(()))
              }
          }.flatten

        def enqueueAssignment(
          streamId: StreamId,
          assigned: SortedSet[TopicPartition],
          partitionsMapQueue: PartitionsMapQueue,
          assignmentRevoked: F[Unit]
        ): F[Unit] = {
          val assignment: F[PartitionsMap] = if (assigned.isEmpty) {
            F.pure(Map.empty)
          } else {
            assigned.toVector
              .traverse { partition =>
                createPartitionStream(streamId, partition, assignmentRevoked).map { stream =>
                  partition -> stream
                }
              }
              .map(_.toMap)
          }

          assignment.flatMap { assignment =>
            stopConsumingDeferred.tryGet.flatMap {
              case None =>
                partitionsMapQueue.enqueue1(Some(assignment))
              case Some(()) =>
                F.unit
            }
          }
        }

        def onRebalance(
          streamId: StreamId,
          prevAssignmentFinisherRef: Ref[F, Deferred[F, Unit]],
          partitionsMapQueue: PartitionsMapQueue
        ): OnRebalance[F, K, V] =
          OnRebalance(
            onRevoked = _ => {
              for {
                newFinisher <- Deferred[F, Unit]
                prevAssignmentFinisher <- prevAssignmentFinisherRef.getAndSet(newFinisher)
                _ <- prevAssignmentFinisher.complete(())
              } yield ()
            },
            onAssigned = assigned => {
              prevAssignmentFinisherRef.get.flatMap { prevAssignmentFinisher =>
                enqueueAssignment(
                  streamId = streamId,
                  assigned = assigned,
                  partitionsMapQueue = partitionsMapQueue,
                  assignmentRevoked = prevAssignmentFinisher.get
                )
              }
            }
          )

        def requestAssignment(
          streamId: StreamId,
          prevAssignmentFinisherRef: Ref[F, Deferred[F, Unit]],
          partitionsMapQueue: PartitionsMapQueue
        ): F[SortedSet[TopicPartition]] =
          Deferred[F, Either[Throwable, SortedSet[TopicPartition]]].flatMap { deferred =>
            val request =
              Request.Assignment[F, K, V](
                deferred.complete,
                Some(onRebalance(streamId, prevAssignmentFinisherRef, partitionsMapQueue))
              )
            val assignment = requests.enqueue1(request) >> deferred.get.rethrow
            F.race(awaitTermination.attempt, assignment).map {
              case Left(_)         => SortedSet.empty[TopicPartition]
              case Right(assigned) => assigned
            }
          }

        def initialEnqueue(
          streamId: StreamId,
          partitionsMapQueue: PartitionsMapQueue,
          prevAssignmentFinisherRef: Ref[F, Deferred[F, Unit]]
        ): F[Unit] =
          for {
            prevAssignmentFinisher <- prevAssignmentFinisherRef.get
            assigned <- requestAssignment(streamId, prevAssignmentFinisherRef, partitionsMapQueue)
            assignmentRevoked = prevAssignmentFinisher.get
            _ <- enqueueAssignment(streamId, assigned, partitionsMapQueue, assignmentRevoked)
          } yield ()

        Stream.eval(stopConsumingDeferred.tryGet).flatMap {
          case None =>
            for {
              partitionsMapQueue <- Stream.eval(Queue.noneTerminated[F, PartitionsMap])
              streamId <- Stream.eval(streamIdRef.modify(n => (n + 1, n)))
              prevAssignmentFinisher <- Stream.eval(Deferred[F, Unit])
              prevAssignmentFinisherRef <- Stream.eval(Ref[F].of(prevAssignmentFinisher))
              _ <- Stream
                .eval(initialEnqueue(streamId, partitionsMapQueue, prevAssignmentFinisherRef))
              out <- partitionsMapQueue.dequeue
                .interruptWhen(awaitTermination.attempt)
                .concurrently(
                  Stream.eval(stopConsumingDeferred.get >> partitionsMapQueue.enqueue1(None))
                )
            } yield out

          case Some(()) =>
            Stream.empty.covaryAll[F, PartitionsMap]
        }
      }

      override def partitionedStream: Stream[F, Stream[F, CommittableConsumerRecord[F, K, V]]] =
        partitionsMapStream.flatMap { partitionsMap =>
          Stream.emits(partitionsMap.toVector.map {
            case (_, partitionStream) =>
              partitionStream
          })
        }

      override def stream: Stream[F, CommittableConsumerRecord[F, K, V]] =
        partitionedStream.parJoinUnbounded

      override def commitAsync(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] =
        request { callback =>
          Request.ManualCommitAsync(
            callback = callback,
            offsets = offsets
          )
        }

      override def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] =
        request { callback =>
          Request.ManualCommitSync(
            callback = callback,
            offsets = offsets
          )
        }

      private[this] def request[A](
        request: (Either[Throwable, A] => F[Unit]) => Request[F, K, V]
      ): F[A] =
        Deferred[F, Either[Throwable, A]].flatMap { deferred =>
          requests.enqueue1(request(deferred.complete)) >>
            F.race(awaitTermination.as(ConsumerShutdownException()), deferred.get.rethrow)
        }.rethrow

      override def assignment: F[SortedSet[TopicPartition]] =
        assignment(Option.empty)

      private def assignment(
        onRebalance: Option[OnRebalance[F, K, V]]
      ): F[SortedSet[TopicPartition]] =
        request { callback =>
          Request.Assignment(
            callback = callback,
            onRebalance = onRebalance
          )
        }

      override def assignmentStream: Stream[F, SortedSet[TopicPartition]] = {
        // NOTE: `initialAssignmentDone` is needed here to guard against the
        // race condition when a rebalance triggers after the listeners are
        // registered but before `assignmentRef` can be updated with initial
        // assignments.
        def onRebalanceWith(
          updateQueue: Queue[F, SortedSet[TopicPartition]],
          assignmentRef: Ref[F, SortedSet[TopicPartition]],
          initialAssignmentDone: F[Unit]
        ): OnRebalance[F, K, V] =
          OnRebalance(
            onAssigned = assigned =>
              initialAssignmentDone >>
                assignmentRef
                  .updateAndGet(_ ++ assigned)
                  .flatMap(updateQueue.enqueue1),
            onRevoked = revoked =>
              initialAssignmentDone >>
                assignmentRef
                  .updateAndGet(_ -- revoked)
                  .flatMap(updateQueue.enqueue1)
          )

        Stream.eval {
          (
            Queue.unbounded[F, SortedSet[TopicPartition]],
            Ref[F].of(SortedSet.empty[TopicPartition]),
            Deferred[F, Unit]
          ).tupled.flatMap[Stream[F, SortedSet[TopicPartition]]] {
            case (updateQueue, assignmentRef, initialAssignmentDeferred) =>
              val onRebalance =
                onRebalanceWith(
                  updateQueue = updateQueue,
                  assignmentRef = assignmentRef,
                  initialAssignmentDone = initialAssignmentDeferred.get
                )

              assignment(Some(onRebalance))
                .flatMap { initialAssignment =>
                  assignmentRef.set(initialAssignment) >>
                    updateQueue.enqueue1(initialAssignment) >>
                    initialAssignmentDeferred.complete(())
                }
                .as(updateQueue.dequeue.changes)
          }
        }.flatten
      }

      override def seek(partition: TopicPartition, offset: Long): F[Unit] =
        withConsumer.blocking { _.seek(partition, offset) }

      override def seekToBeginning: F[Unit] =
        seekToBeginning(List.empty[TopicPartition])

      override def seekToBeginning[G[_]](partitions: G[TopicPartition])(
        implicit G: Foldable[G]
      ): F[Unit] =
        withConsumer.blocking { _.seekToBeginning(partitions.asJava) }

      override def seekToEnd: F[Unit] =
        seekToEnd(List.empty[TopicPartition])

      override def seekToEnd[G[_]](
        partitions: G[TopicPartition]
      )(implicit G: Foldable[G]): F[Unit] =
        withConsumer.blocking { _.seekToEnd(partitions.asJava) }

      override def partitionsFor(
        topic: String
      ): F[List[PartitionInfo]] =
        withConsumer.blocking { _.partitionsFor(topic).asScala.toList }

      override def partitionsFor(
        topic: String,
        timeout: FiniteDuration
      ): F[List[PartitionInfo]] =
        withConsumer.blocking { _.partitionsFor(topic, timeout.asJava).asScala.toList }

      override def position(partition: TopicPartition): F[Long] =
        withConsumer.blocking { _.position(partition) }

      override def position(partition: TopicPartition, timeout: FiniteDuration): F[Long] =
        withConsumer.blocking { _.position(partition, timeout.asJava) }

      override def subscribeTo(firstTopic: String, remainingTopics: String*): F[Unit] =
        subscribe(NonEmptyList.of(firstTopic, remainingTopics: _*))

      override def subscribe[G[_]](topics: G[String])(implicit G: Reducible[G]): F[Unit] =
        request { callback =>
          Request.SubscribeTopics(
            topics = topics.toNonEmptyList,
            callback = callback
          )
        }

      override def subscribe(regex: Regex): F[Unit] =
        request { callback =>
          Request.SubscribePattern(
            pattern = regex.pattern,
            callback = callback
          )
        }

      override def unsubscribe: F[Unit] =
        request { callback =>
          Request.Unsubscribe(
            callback = callback
          )
        }

      override def stopConsuming: F[Unit] =
        stopConsumingDeferred.complete(()).attempt.void

      override def assign(partitions: NonEmptySet[TopicPartition]): F[Unit] =
        request { callback =>
          Request.Assign(
            topicPartitions = partitions,
            callback = callback
          )
        }

      override def assign(topic: String, partitions: NonEmptySet[Int]): F[Unit] =
        assign(partitions.map(new TopicPartition(topic, _)))

      override def assign(topic: String): F[Unit] =
        for {
          partitions <- partitionsFor(topic)
            .map { partitionInfo =>
              NonEmptySet.fromSet {
                SortedSet(partitionInfo.map(_.partition): _*)
              }
            }
          _ <- partitions.fold(F.unit)(assign(topic, _))
        } yield ()

      override def beginningOffsets(
        partitions: Set[TopicPartition]
      ): F[Map[TopicPartition, Long]] =
        withConsumer.blocking {
          _.beginningOffsets(partitions.asJava)
            .asInstanceOf[util.Map[TopicPartition, Long]]
            .toMap
        }

      override def beginningOffsets(
        partitions: Set[TopicPartition],
        timeout: FiniteDuration
      ): F[Map[TopicPartition, Long]] =
        withConsumer.blocking {
          _.beginningOffsets(partitions.asJava, timeout.asJava)
            .asInstanceOf[util.Map[TopicPartition, Long]]
            .toMap
        }

      override def endOffsets(
        partitions: Set[TopicPartition]
      ): F[Map[TopicPartition, Long]] =
        withConsumer.blocking {
          _.endOffsets(partitions.asJava)
            .asInstanceOf[util.Map[TopicPartition, Long]]
            .toMap
        }

      override def endOffsets(
        partitions: Set[TopicPartition],
        timeout: FiniteDuration
      ): F[Map[TopicPartition, Long]] =
        withConsumer.blocking {
          _.endOffsets(partitions.asJava, timeout.asJava)
            .asInstanceOf[util.Map[TopicPartition, Long]]
            .toMap
        }

      override def metrics: F[Map[MetricName, Metric]] =
        withConsumer.blocking { _.metrics().asScala.toMap }

      override def toString: String =
        "KafkaConsumer$" + id

    }

  @deprecated("use KafkaConsumer.resource", "1.2.0")
  def consumerResource[F[_], K, V](
    settings: ConsumerSettings[F, K, V]
  )(
    implicit F: ConcurrentEffect[F],
    timer: Temporal[F]
  ): Resource[F, KafkaConsumer[F, K, V]] = resource(settings)

  /**
    * Creates a new [[KafkaConsumer]] in the `Resource` context,
    * using the specified [[ConsumerSettings]]. Note that there
    * is another version where `F[_]` is specified explicitly and
    * the key and value type can be inferred, which allows you
    * to use the following syntax.
    *
    * {{{
    * KafkaConsumer.resource[F].using(settings)
    * }}}
    */
  def resource[F[_], K, V](
    settings: ConsumerSettings[F, K, V]
  )(
    implicit F: ConcurrentEffect[F],
    timer: Temporal[F]
  ): Resource[F, KafkaConsumer[F, K, V]] =
    for {
      keyDeserializer <- Resource.eval(settings.keyDeserializer)
      valueDeserializer <- Resource.eval(settings.valueDeserializer)
      id <- Resource.eval(F.delay(new Object().hashCode))
      jitter <- Resource.eval(Jitter.default[F])
      logging <- Resource.eval(Logging.default[F](id))
      requests <- Resource.eval(Queue.unbounded[F, Request[F, K, V]])
      polls <- Resource.eval(Queue.bounded[F, Request[F, K, V]](1))
      ref <- Resource.eval(Ref.of[F, State[F, K, V]](State.empty))
      streamId <- Resource.eval(Ref.of[F, StreamId](0))
      stopConsumingDeferred <- Resource.eval(Deferred.tryable[F, Unit])
      withConsumer <- WithConsumer(settings)
      actor = {
        implicit val jitter0: Jitter[F] = jitter
        implicit val logging0: Logging[F] = logging

        new KafkaConsumerActor(
          settings = settings,
          keyDeserializer = keyDeserializer,
          valueDeserializer = valueDeserializer,
          ref = ref,
          requests = requests,
          withConsumer = withConsumer
        )
      }
      actor <- startConsumerActor(requests, polls, actor)
      polls <- startPollScheduler(polls, settings.pollInterval)
    } yield createKafkaConsumer(
      requests,
      settings,
      actor,
      polls,
      streamId,
      id,
      withConsumer,
      stopConsumingDeferred
    )

  /**
    * Alternative version of `resource` where the `F[_]` is
    * specified explicitly, and where the key and value type can
    * be inferred from the [[ConsumerSettings]]. This allows you
    * to use the following syntax.
    *
    * {{{
    * KafkaConsumer.resource[F].using(settings)
    * }}}
    */
  @deprecated("use KafkaConsumer[F].resource(settings)", "1.5.0")
  def resource[F[_]](implicit F: ConcurrentEffect[F]): ConsumerResource[F] = new ConsumerResource(F)

  /**
    * Creates a new [[KafkaConsumer]] in the `Stream` context,
    * using the specified [[ConsumerSettings]]. Note that there
    * is another version where `F[_]` is specified explicitly and
    * the key and value type can be inferred, which allows you
    * to use the following syntax.
    *
    * {{{
    * KafkaConsumer.stream[F].using(settings)
    * }}}
    */
  def stream[F[_], K, V](settings: ConsumerSettings[F, K, V])(
    implicit F: ConcurrentEffect[F],
    timer: Temporal[F]
  ): Stream[F, KafkaConsumer[F, K, V]] =
    Stream.resource(resource(settings))

  /**
    * Alternative version of `stream` where the `F[_]` is
    * specified explicitly, and where the key and value type can
    * be inferred from the [[ConsumerSettings]]. This allows you
    * to use the following syntax.
    *
    * {{{
    * KafkaConsumer.stream[F].using(settings)
    * }}}
    */
  @deprecated("use KafkaConsumer[F].stream(settings)", "1.5.0")
  def stream[F[_]](implicit F: ConcurrentEffect[F]): ConsumerStream[F] =
    new ConsumerStream[F](F)

  def apply[F[_]]: ConsumerPartiallyApplied[F] =
    new ConsumerPartiallyApplied()

  private[kafka] final class ConsumerPartiallyApplied[F[_]](val dummy: Boolean = true)
      extends AnyVal {

    /**
      * Alternative version of `resource` where the `F[_]` is
      * specified explicitly, and where the key and value type can
      * be inferred from the [[ConsumerSettings]]. This allows you
      * to use the following syntax.
      *
      * {{{
      * KafkaConsumer[F].resource(settings)
      * }}}
      */
    def resource[K, V](settings: ConsumerSettings[F, K, V])(
      implicit F: ConcurrentEffect[F],
      timer: Temporal[F]
    ): Resource[F, KafkaConsumer[F, K, V]] =
      KafkaConsumer.resource(settings)

    /**
      * Alternative version of `stream` where the `F[_]` is
      * specified explicitly, and where the key and value type can
      * be inferred from the [[ConsumerSettings]]. This allows you
      * to use the following syntax.
      *
      * {{{
      * KafkaConsumer[F].stream(settings)
      * }}}
      */
    def stream[K, V](settings: ConsumerSettings[F, K, V])(
      implicit F: ConcurrentEffect[F],
      timer: Temporal[F]
    ): Stream[F, KafkaConsumer[F, K, V]] =
      KafkaConsumer.stream(settings)

    override def toString: String =
      "ConsumerPartiallyApplied$" + System.identityHashCode(this)
  }

  /*
   * Extension methods for operating on a `KafkaConsumer` in a `Stream` context without needing
   * to explicitly use operations such as `flatMap` and `evalTap`
   */
  implicit final class StreamOps[F[_]: Functor, K, V](self: Stream[F, KafkaConsumer[F, K, V]]) {

    /**
      * Subscribes a consumer to the specified topics within the [[Stream]] context.
      * See [[KafkaSubscription#subscribe]].
      */
    def subscribe[G[_]: Reducible](topics: G[String]): Stream[F, KafkaConsumer[F, K, V]] =
      self.evalTap(_.subscribe(topics))

    def subscribe(regex: Regex): Stream[F, KafkaConsumer[F, K, V]] =
      self.evalTap(_.subscribe(regex))

    /**
      * Subscribes a consumer to the specified topics within the [[Stream]] context.
      * See [[KafkaSubscription#subscribe]].
      */
    def subscribeTo(
      firstTopic: String,
      remainingTopics: String*
    ): Stream[F, KafkaConsumer[F, K, V]] =
      self.evalTap(_.subscribeTo(firstTopic, remainingTopics: _*))

    /**
      * A [[Stream]] of records from the allocated [[KafkaConsumer]]. Alias for [[stream]].
      * See [[KafkaConsume#stream]]
      */
    def records: Stream[F, CommittableConsumerRecord[F, K, V]] = stream

    /**
      * A [[Stream]] of records from the allocated [[KafkaConsumer]].
      * See [[KafkaConsume#stream]]
      */
    def stream: Stream[F, CommittableConsumerRecord[F, K, V]] = self.flatMap(_.records)

    /**
      * Alias for [[partitionedStream]]. See [[KafkaConsume#partitionedStream]]
      */
    def partitionedRecords: Stream[F, Stream[F, CommittableConsumerRecord[F, K, V]]] =
      partitionedStream

    /**
      * See [[KafkaConsume#partitionedStream]]
      */
    def partitionedStream: Stream[F, Stream[F, CommittableConsumerRecord[F, K, V]]] =
      self.flatMap(_.partitionedRecords)
  }
}

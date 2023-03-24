/*
 * Copyright 2018-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.{Foldable, Functor, Reducible}
import cats.data.{NonEmptySet, OptionT}
import cats.effect._
import cats.effect.std._
import cats.effect.implicits._
import cats.syntax.all._
import fs2.{Chunk, Stream}
import fs2.kafka.internal._
import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters._
import fs2.kafka.instances._
import fs2.kafka.internal.KafkaConsumerActor._
import fs2.kafka.internal.syntax._
import fs2.kafka.consumer._
import fs2.kafka.internal.LogEntry.{RevokedPreviousFetch, StoredFetch}

import java.util
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.{Metric, MetricName, PartitionInfo, TopicPartition}

import scala.annotation.nowarn
import scala.collection.immutable.SortedSet
import scala.concurrent.duration.FiniteDuration
import scala.util.matching.Regex

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
    with KafkaOffsetsV2[F]
    with KafkaSubscription[F]
    with KafkaTopics[F]
    with KafkaCommit[F]
    with KafkaMetrics[F]
    with KafkaConsumerLifecycle[F]

object KafkaConsumer {
  private def spawnRepeating[F[_]: Concurrent, A](fa: F[A]): Resource[F, FakeFiber[F]] =
    Resource.make {
      Deferred[F, Either[Throwable, Unit]].flatMap { deferred =>
        fa.foreverM[Unit]
          .guaranteeCase {
            case Outcome.Errored(e) => deferred.complete(Left(e)).void
            case _                  => deferred.complete(Right(())).void
          }
          .start
          .map(fiber => FakeFiber(deferred.get.rethrow, fiber.cancel.start.void))
      }
    }(_.cancel)

  private def startConsumerActor[F[_], K, V](
    requests: QueueSource[F, Request[F, K, V]],
    polls: QueueSource[F, Request.Poll[F]],
    actor: KafkaConsumerActor[F, K, V]
  )(
    implicit F: Async[F]
  ): Resource[F, FakeFiber[F]] =
    spawnRepeating {
      OptionT(requests.tryTake)
        .getOrElseF(polls.take.widen)
        .flatMap(actor.handle(_))
    }

  private def startPollScheduler[F[_], K, V](
    polls: QueueSink[F, Request.Poll[F]],
    pollInterval: FiniteDuration
  )(
    implicit F: Temporal[F]
  ): Resource[F, FakeFiber[F]] =
    spawnRepeating {
      polls.offer(Request.poll) >> F.sleep(pollInterval)
    }

  private def createKafkaConsumer[F[_], K, V](
    requests: QueueSink[F, Request[F, K, V]],
    settings: ConsumerSettings[F, K, V],
    actor: KafkaConsumerActor[F, K, V],
    fiber: FakeFiber[F],
    streamIdRef: Ref[F, StreamId],
    id: Int,
    withConsumer: WithConsumer[F],
    stopConsumingDeferred: Deferred[F, Unit]
  )(implicit F: Async[F], logging: Logging[F]): KafkaConsumer[F, K, V] =
    new KafkaConsumer[F, K, V] {

      override def partitionsMapStream
        : Stream[F, Map[TopicPartition, Stream[F, CommittableConsumerRecord[F, K, V]]]] = {
        val chunkQueue: F[Queue[F, Option[Chunk[CommittableConsumerRecord[F, K, V]]]]] =
          Queue.bounded(settings.maxPrefetchBatches - 1)

        type PartitionResult =
          (Chunk[CommittableConsumerRecord[F, K, V]], FetchCompletedReason)

        type PartitionsMap = Map[TopicPartition, Stream[F, CommittableConsumerRecord[F, K, V]]]
        type PartitionsMapQueue = Queue[F, Option[PartitionsMap]]

        def partitionStream(
          streamId: StreamId,
          partition: TopicPartition,
          assignmentRevoked: F[Unit]
        ): Stream[F, CommittableConsumerRecord[F, K, V]] = Stream.force {
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
            stopReqs <- Deferred[F, Unit]
          } yield Stream.eval {
            def fetchPartition: F[Unit] = F.deferred[PartitionResult].flatMap { deferred =>
              val callback: PartitionResult => F[Unit] =
                deferred.complete(_).void

              val fetch: F[PartitionResult] = withPermit {
                val assigned =
                  withConsumer.blocking {
                    _.assignment.contains(partition)
                  }

                def storeFetch: F[Unit] =
                  actor.ref.modify { state =>
                    val (newState, oldFetches) =
                      state.withFetch(partition, streamId, callback)
                    newState ->
                      (logging.log(StoredFetch(partition, callback, newState)) >>
                        oldFetches.traverse_ { fetch =>
                          fetch.completeRevoked(Chunk.empty) >>
                            logging.log(RevokedPreviousFetch(partition, streamId))
                        })
                  }.flatten

                def completeRevoked: F[Unit] =
                  callback((Chunk.empty, FetchCompletedReason.TopicPartitionRevoked))

                assigned.ifM(storeFetch, completeRevoked)
              } >> deferred.get
              F.race(shutdown, fetch).flatMap {
                case Left(()) =>
                  stopReqs.complete(()).void

                case Right((chunk, reason)) =>
                  val enqueueChunk = chunks.offer(Some(chunk)).unlessA(chunk.isEmpty)

                  val completeRevoked =
                    stopReqs.complete(()).void.whenA(reason.topicPartitionRevoked)

                  enqueueChunk >> completeRevoked
              }
            }

            Stream
              .repeatEval {
                stopReqs.tryGet.flatMap {
                  case None =>
                    fetchPartition

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
              .guarantee(F.race(dequeueDone.get, chunks.offer(None)).void)
              .start
              .as {
                Stream
                  .fromQueueNoneTerminated(chunks)
                  .flatMap(Stream.chunk)
                  .covary[F]
                  .onFinalize(dequeueDone.complete(()).void)
              }
          }.flatten
        }

        def enqueueAssignment(
          streamId: StreamId,
          assigned: Map[TopicPartition, Deferred[F, Unit]],
          partitionsMapQueue: PartitionsMapQueue
        ): F[Unit] =
          stopConsumingDeferred.tryGet.flatMap {
            case None =>
              val assignment: PartitionsMap = assigned.map {
                case (partition, finisher) =>
                  partition -> partitionStream(streamId, partition, finisher.get)
              }
              partitionsMapQueue.offer(Some(assignment))
            case Some(()) =>
              F.unit
          }

        def onRebalance(
          streamId: StreamId,
          assignmentRef: Ref[F, Map[TopicPartition, Deferred[F, Unit]]],
          partitionsMapQueue: PartitionsMapQueue
        ): OnRebalance[F] =
          OnRebalance(
            onRevoked = revoked => {
              for {
                finishers <- assignmentRef.modify(_.partition(entry => !revoked.contains(entry._1)))
                _ <- finishers.toVector
                  .traverse {
                    case (_, finisher) =>
                      finisher.complete(())
                  }
              } yield ()
            },
            onAssigned = assignedPartitions => {
              for {
                assignment <- assignedPartitions.toVector
                  .traverse { partition =>
                    Deferred[F, Unit].map(partition -> _)
                  }
                  .map(_.toMap)
                _ <- assignmentRef.update(_ ++ assignment)
                _ <- enqueueAssignment(
                  streamId = streamId,
                  assigned = assignment,
                  partitionsMapQueue = partitionsMapQueue
                )
              } yield ()
            }
          )

        def requestAssignment(
          streamId: StreamId,
          assignmentRef: Ref[F, Map[TopicPartition, Deferred[F, Unit]]],
          partitionsMapQueue: PartitionsMapQueue
        ): F[Map[TopicPartition, Deferred[F, Unit]]] = {
          val assignment = this.assignment(
            Some(
              onRebalance(
                streamId,
                assignmentRef,
                partitionsMapQueue
              )
            )
          )

          F.race(awaitTermination.attempt, assignment).flatMap {
            case Left(_) =>
              F.pure(Map.empty)

            case Right(assigned) =>
              assigned.toVector
                .traverse { partition =>
                  Deferred[F, Unit].map(partition -> _)
                }
                .map(_.toMap)
          }
        }

        def initialEnqueue(
          streamId: StreamId,
          assignmentRef: Ref[F, Map[TopicPartition, Deferred[F, Unit]]],
          partitionsMapQueue: PartitionsMapQueue
        ): F[Unit] =
          for {
            assigned <- requestAssignment(
              streamId,
              assignmentRef,
              partitionsMapQueue
            )
            _ <- enqueueAssignment(streamId, assigned, partitionsMapQueue)
          } yield ()

        Stream.eval(stopConsumingDeferred.tryGet).flatMap {
          case None =>
            for {
              partitionsMapQueue <- Stream.eval(Queue.unbounded[F, Option[PartitionsMap]])
              streamId <- Stream.eval(streamIdRef.modify(n => (n + 1, n)))
              assignmentRef <- Stream.eval(Ref[F].of(Map.empty[TopicPartition, Deferred[F, Unit]]))
              _ <- Stream
                .eval(
                  initialEnqueue(
                    streamId,
                    assignmentRef,
                    partitionsMapQueue
                  )
                )
              out <- Stream
                .fromQueueNoneTerminated(partitionsMapQueue)
                .interruptWhen(awaitTermination.attempt)
                .concurrently(
                  Stream.eval(stopConsumingDeferred.get >> partitionsMapQueue.offer(None))
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
          requests.offer(request(deferred.complete(_).void)) >>
            F.race(awaitTermination.as(ConsumerShutdownException()), deferred.get.rethrow)
        }.rethrow

      override def assignment: F[SortedSet[TopicPartition]] =
        assignment(Option.empty)

      private def assignment(
        onRebalance: Option[OnRebalance[F]]
      ): F[SortedSet[TopicPartition]] =
        withPermit {
          onRebalance
            .fold(actor.ref.updateAndGet(_.asStreaming)) { on =>
              actor.ref.updateAndGet(_.withOnRebalance(on).asStreaming).flatTap { newState =>
                logging.log(LogEntry.StoredOnRebalance(on, newState))
              }

            }
            .ensure(NotSubscribedException())(_.subscribed) >>
            withConsumer.blocking(_.assignment.toSortedSet)
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
        ): OnRebalance[F] =
          OnRebalance(
            onAssigned = assigned =>
              initialAssignmentDone >>
                assignmentRef
                  .updateAndGet(_ ++ assigned)
                  .flatMap(updateQueue.offer),
            onRevoked = revoked =>
              initialAssignmentDone >>
                assignmentRef
                  .updateAndGet(_ -- revoked)
                  .flatMap(updateQueue.offer)
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
                    updateQueue.offer(initialAssignment) >>
                    initialAssignmentDeferred.complete(())
                }
                .as(Stream.fromQueueUnterminated(updateQueue).changes)
          }
        }.flatten
      }

      override def seek(partition: TopicPartition, offset: Long): F[Unit] =
        withConsumer.blocking { _.seek(partition, offset) }

      override def seekToBeginning[G[_]](partitions: G[TopicPartition])(
        implicit G: Foldable[G]
      ): F[Unit] =
        withConsumer.blocking { _.seekToBeginning(partitions.asJava) }

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
        withConsumer.blocking { _.partitionsFor(topic, timeout.toJava).asScala.toList }

      override def position(partition: TopicPartition): F[Long] =
        withConsumer.blocking { _.position(partition) }

      override def position(partition: TopicPartition, timeout: FiniteDuration): F[Long] =
        withConsumer.blocking { _.position(partition, timeout.toJava) }

      override def committed(
        partitions: Set[TopicPartition]
      ): F[Map[TopicPartition, OffsetAndMetadata]] =
        withConsumer.blocking {
          _.committed(partitions.asJava)
            .asInstanceOf[util.Map[TopicPartition, OffsetAndMetadata]]
            .toMap
        }

      override def committed(
        partitions: Set[TopicPartition],
        timeout: FiniteDuration
      ): F[Map[TopicPartition, OffsetAndMetadata]] =
        withConsumer.blocking {
          _.committed(partitions.asJava, timeout.toJava)
            .asInstanceOf[util.Map[TopicPartition, OffsetAndMetadata]]
            .toMap
        }

      override def subscribe[G[_]](topics: G[String])(implicit G: Reducible[G]): F[Unit] =
        withPermit {
          withConsumer.blocking {
            _.subscribe(
              topics.toList.asJava,
              actor.consumerRebalanceListener
            )
          } >> actor.ref
            .updateAndGet(_.asSubscribed)
            .log(LogEntry.SubscribedTopics(topics.toNonEmptyList, _))
        }

      private def withPermit[A](fa: F[A]): F[A] = F.deferred[Either[Throwable, A]].flatMap {
        deferred =>
          requests
            .offer(Request.WithPermit(fa, deferred.complete(_: Either[Throwable, A]).void)) >> deferred.get.rethrow
      }

      override def subscribe(regex: Regex): F[Unit] =
        withPermit {
          withConsumer.blocking {
            _.subscribe(
              regex.pattern,
              actor.consumerRebalanceListener
            )
          } >>
            actor.ref
              .updateAndGet(_.asSubscribed)
              .log(LogEntry.SubscribedPattern(regex.pattern, _))
        }

      override def unsubscribe: F[Unit] =
        withPermit {
          withConsumer.blocking { _.unsubscribe() } >> actor.ref
            .updateAndGet(_.asUnsubscribed)
            .log(LogEntry.Unsubscribed(_))
        }

      override def stopConsuming: F[Unit] =
        stopConsumingDeferred.complete(()).attempt.void

      override def assign(partitions: NonEmptySet[TopicPartition]): F[Unit] =
        withPermit {
          withConsumer.blocking {
            _.assign(
              partitions.toList.asJava
            )
          } >> actor.ref
            .updateAndGet(_.asSubscribed)
            .log(LogEntry.ManuallyAssignedPartitions(partitions, _))

        }

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
          _.beginningOffsets(partitions.asJava, timeout.toJava)
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
          _.endOffsets(partitions.asJava, timeout.toJava)
            .asInstanceOf[util.Map[TopicPartition, Long]]
            .toMap
        }

      override def metrics: F[Map[MetricName, Metric]] =
        withConsumer.blocking { _.metrics().asScala.toMap }

      override def toString: String =
        "KafkaConsumer$" + id

      override def terminate: F[Unit] = fiber.cancel

      override def awaitTermination: F[Unit] = fiber.join
    }

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
    implicit F: Async[F],
    mk: MkConsumer[F]
  ): Resource[F, KafkaConsumer[F, K, V]] =
    for {
      keyDeserializer <- Resource.eval(settings.keyDeserializer)
      valueDeserializer <- Resource.eval(settings.valueDeserializer)
      id <- Resource.eval(F.delay(new Object().hashCode))
      jitter <- Resource.eval(Jitter.default[F])
      logging <- Resource.eval(Logging.default[F](id))
      requests <- Resource.eval(Queue.unbounded[F, Request[F, K, V]])
      polls <- Resource.eval(Queue.bounded[F, Request.Poll[F]](1))
      ref <- Resource.eval(Ref.of[F, State[F, K, V]](State.empty))
      streamId <- Resource.eval(Ref.of[F, StreamId](0))
      dispatcher <- Dispatcher[F]
      stopConsumingDeferred <- Resource.eval(Deferred[F, Unit])
      withConsumer <- WithConsumer(mk, settings)
      actor = {
        implicit val jitter0: Jitter[F] = jitter
        implicit val logging0: Logging[F] = logging
        implicit val dispatcher0: Dispatcher[F] = dispatcher

        new KafkaConsumerActor(
          settings = settings,
          keyDeserializer = keyDeserializer,
          valueDeserializer = valueDeserializer,
          ref = ref,
          requests = requests,
          withConsumer = withConsumer
        )
      }
      actorFiber <- startConsumerActor(requests, polls, actor)
      polls <- startPollScheduler(polls, settings.pollInterval)
    } yield createKafkaConsumer(
      requests,
      settings,
      actor,
      actorFiber.combine(polls),
      streamId,
      id,
      withConsumer,
      stopConsumingDeferred
    )(F, logging)

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
  def stream[F[_], K, V](
    settings: ConsumerSettings[F, K, V]
  )(implicit F: Async[F], mk: MkConsumer[F]): Stream[F, KafkaConsumer[F, K, V]] =
    Stream.resource(resource(settings))

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
      implicit F: Async[F],
      mk: MkConsumer[F]
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
      implicit F: Async[F],
      mk: MkConsumer[F]
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

  /*
   * Prevents the default `MkConsumer` instance from being implicitly available
   * to code defined in this object, ensuring factory methods require an instance
   * to be provided at the call site.
   */
  @nowarn("msg=never used")
  implicit private def mkAmbig1[F[_]]: MkConsumer[F] =
    throw new AssertionError("should not be used")
  @nowarn("msg=never used")
  implicit private def mkAmbig2[F[_]]: MkConsumer[F] =
    throw new AssertionError("should not be used")
}

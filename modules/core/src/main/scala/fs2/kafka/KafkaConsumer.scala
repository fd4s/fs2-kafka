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

package fs2.kafka

import cats.{Foldable, Reducible}
import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect._
import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.implicits._
import cats.implicits._
import fs2.{Chunk, Stream}
import fs2.concurrent.Queue
import fs2.kafka.internal._
import fs2.kafka.internal.converters.collection._
import fs2.kafka.internal.instances._
import fs2.kafka.internal.KafkaConsumerActor._
import fs2.kafka.internal.syntax._
import java.util
import org.apache.kafka.common.TopicPartition
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
sealed abstract class KafkaConsumer[F[_], K, V] {

  /**
    * Alias for `partitionedStream.parJoinUnbounded`.
    * See [[partitionedStream]] for more information.
    *
    * @note you have to first use `subscribe` to subscribe the consumer
    *       before using this `Stream`. If you forgot to subscribe, there
    *       will be a [[NotSubscribedException]] raised in the `Stream`.
    */
  def stream: Stream[F, CommittableConsumerRecord[F, K, V]]

  /**
    * `Stream` where the elements themselves are `Stream`s which continually
    * request records for a single partition. These `Stream`s will have to be
    * processed in parallel, using `parJoin` or `parJoinUnbounded`. Note that
    * when using `parJoin(n)` and `n` is smaller than the number of currently
    * assigned partitions, then there will be assigned partitions which won't
    * be processed. For that reason, prefer `parJoinUnbounded` and the actual
    * limit will be the number of assigned partitions.<br>
    * <br>
    * If you do not want to process all partitions in parallel, then you
    * can use [[stream]] instead, where records for all partitions are in
    * a single `Stream`.
    *
    * @note you have to first use `subscribe` to subscribe the consumer
    *       before using this `Stream`. If you forgot to subscribe, there
    *       will be a [[NotSubscribedException]] raised in the `Stream`.
    */
  def partitionedStream: Stream[F, Stream[F, CommittableConsumerRecord[F, K, V]]]

  /**
    * Returns the set of partitions currently assigned to this consumer.
    */
  def assignment: F[SortedSet[TopicPartition]]

  /**
    * `Stream` where the elements are the set of `TopicPartition`s currently
    * assigned to this consumer. The stream emits whenever a rebalance changes
    * partition assignments.
    */
  def assignmentStream: Stream[F, SortedSet[TopicPartition]]

  /**
    * Overrides the fetch offsets that the consumer will use when reading the
    * next record. If this API is invoked for the same partition more than once,
    * the latest offset will be used. Note that you may lose data if this API is
    * arbitrarily used in the middle of consumption to reset the fetch offsets.
    */
  def seek(partition: TopicPartition, offset: Long): F[Unit]

  /**
    * Seeks to the first offset for each currently assigned partition.
    * This is equivalent to using `seekToBeginning` with an empty set
    * of partitions.<br>
    * <br>
    * Note that this seek evaluates lazily, and only on the next call
    * to `poll` or `position`.
    */
  def seekToBeginning: F[Unit]

  /**
    * Seeks to the first offset for each of the specified partitions.
    * If no partitions are provided, seeks to the first offset for
    * all currently assigned partitions.<br>
    * <br>
    * Note that this seek evaluates lazily, and only on the next call
    * to `poll` or `position`.
    */
  def seekToBeginning[G[_]](partitions: G[TopicPartition])(
    implicit G: Foldable[G]
  ): F[Unit]

  /**
    * Seeks to the last offset for each currently assigned partition.
    * This is equivalent to using `seekToEnd` with an empty set of
    * partitions.<br>
    * <br>
    * Note that this seek evaluates lazily, and only on the next call
    * to `poll` or `position`.
    */
  def seekToEnd: F[Unit]

  /**
    * Seeks to the last offset for each of the specified partitions.
    * If no partitions are provided, seeks to the last offset for
    * all currently assigned partitions.<br>
    * <br>
    * Note that this seek evaluates lazily, and only on the next call
    * to `poll` or `position`.
    */
  def seekToEnd[G[_]](partitions: G[TopicPartition])(
    implicit G: Foldable[G]
  ): F[Unit]

  /**
    * Returns the offset of the next record that will be fetched.<br>
    * <br>
    * Timeout is determined by `default.api.timeout.ms`, which
    * is set using [[ConsumerSettings#withDefaultApiTimeout]].
    */
  def position(partition: TopicPartition): F[Long]

  /**
    * Returns the offset of the next record that will be fetched.
    */
  def position(partition: TopicPartition, timeout: FiniteDuration): F[Long]

  /**
    * Subscribes the consumer to the specified topics. Note that you have to
    * use one of the `subscribe` functions to subscribe to one or more topics
    * before using any of the provided `Stream`s, or a [[NotSubscribedException]]
    * will be raised in the `Stream`s.
    */
  def subscribeTo(firstTopic: String, remainingTopics: String*): F[Unit]

  /**
    * Subscribes the consumer to the specified topics. Note that you have to
    * use one of the `subscribe` functions to subscribe to one or more topics
    * before using any of the provided `Stream`s, or a [[NotSubscribedException]]
    * will be raised in the `Stream`s.
    *
    * @param topics the topics to which the consumer should subscribe
    */
  def subscribe[G[_]](topics: G[String])(implicit G: Reducible[G]): F[Unit]

  /**
    * Subscribes the consumer to the topics matching the specified `Regex`.
    * Note that you have to use one of the `subscribe` functions before you
    * can use any of the provided `Stream`s, or a [[NotSubscribedException]]
    * will be raised in the `Stream`s.
    *
    * @param regex the regex to which matching topics should be subscribed
    */
  def subscribe(regex: Regex): F[Unit]

  /**
    * Returns the first offset for the specified partitions.<br>
    * <br>
    * Timeout is determined by `default.api.timeout.ms`, which
    * is set using [[ConsumerSettings#withDefaultApiTimeout]].
    */
  def beginningOffsets(
    partitions: Set[TopicPartition]
  ): F[Map[TopicPartition, Long]]

  /**
    * Returns the first offset for the specified partitions.
    */
  def beginningOffsets(
    partitions: Set[TopicPartition],
    timeout: FiniteDuration
  ): F[Map[TopicPartition, Long]]

  /**
    * Returns the last offset for the specified partitions.<br>
    * <br>
    * Timeout is determined by `request.timeout.ms`, which
    * is set using [[ConsumerSettings#withRequestTimeout]].
    */
  def endOffsets(
    partitions: Set[TopicPartition]
  ): F[Map[TopicPartition, Long]]

  /**
    * Returns the last offset for the specified partitions.
    */
  def endOffsets(
    partitions: Set[TopicPartition],
    timeout: FiniteDuration
  ): F[Map[TopicPartition, Long]]

  /**
    * A `Fiber` that can be used to cancel the underlying consumer, or
    * wait for it to complete. If you're using [[stream]], or any other
    * provided stream in [[KafkaConsumer]], these will be automatically
    * interrupted when the underlying consumer has been cancelled or
    * when it finishes with an exception.<br>
    * <br>
    * Whenever `cancel` is invoked, an attempt will be made to stop the
    * underlying consumer. The `cancel` operation will not wait for the
    * consumer to shutdown. If you also want to wait for the shutdown
    * to complete, you can use `join`. Note that `join` is guaranteed
    * to complete after consumer shutdown, even when the consumer is
    * cancelled with `cancel`.<br>
    * <br>
    * This `Fiber` instance is usually only required if the consumer
    * needs to be cancelled due to some external condition, or when an
    * external process needs to be cancelled whenever the consumer has
    * shut down. Most of the time, when you're only using the streams
    * provided by [[KafkaConsumer]], there is no need to use this.
    */
  def fiber: Fiber[F, Unit]
}

private[kafka] object KafkaConsumer {
  private[this] def startConsumerActor[F[_], K, V](
    requests: Queue[F, Request[F, K, V]],
    polls: Queue[F, Request[F, K, V]],
    actor: KafkaConsumerActor[F, K, V]
  )(
    implicit F: Concurrent[F],
    context: ContextShift[F]
  ): Resource[F, Fiber[F, Unit]] =
    Resource.make {
      Deferred[F, Either[Throwable, Unit]].flatMap { deferred =>
        F.guaranteeCase {
            requests.tryDequeue1
              .flatMap(_.map(F.pure).getOrElse(polls.dequeue1))
              .flatMap(actor.handle(_) >> context.shift)
              .foreverM[Unit]
          } {
            case ExitCase.Error(e) => deferred.complete(Left(e))
            case _                 => deferred.complete(Right(()))
          }
          .start
          .map(fiber => Fiber[F, Unit](deferred.get.rethrow, fiber.cancel.start.void))
      }
    }(_.cancel)

  private[this] def startPollScheduler[F[_], K, V](
    polls: Queue[F, Request[F, K, V]],
    pollInterval: FiniteDuration
  )(
    implicit F: Concurrent[F],
    timer: Timer[F]
  ): Resource[F, Fiber[F, Unit]] =
    Resource.make {
      Deferred[F, Either[Throwable, Unit]].flatMap { deferred =>
        F.guaranteeCase {
            polls
              .enqueue1(Request.poll)
              .flatMap(_ => timer.sleep(pollInterval))
              .foreverM[Unit]
          } {
            case ExitCase.Error(e) => deferred.complete(Left(e))
            case _                 => deferred.complete(Right(()))
          }
          .start
          .map(fiber => Fiber[F, Unit](deferred.get.rethrow, fiber.cancel.start.void))
      }
    }(_.cancel)

  private[this] def createKafkaConsumer[F[_], K, V](
    requests: Queue[F, Request[F, K, V]],
    settings: ConsumerSettings[F, K, V],
    actor: Fiber[F, Unit],
    polls: Fiber[F, Unit],
    streamIdRef: Ref[F, Int],
    id: Int,
    withConsumer: WithConsumer[F]
  )(implicit F: Concurrent[F]): KafkaConsumer[F, K, V] =
    new KafkaConsumer[F, K, V] {
      override val fiber: Fiber[F, Unit] = {
        val actorFiber =
          Fiber[F, Unit](F.guaranteeCase(actor.join) {
            case ExitCase.Completed => polls.cancel
            case _                  => F.unit
          }, actor.cancel)

        val pollsFiber =
          Fiber[F, Unit](F.guaranteeCase(polls.join) {
            case ExitCase.Completed => actor.cancel
            case _                  => F.unit
          }, polls.cancel)

        actorFiber combine pollsFiber
      }

      override def partitionedStream: Stream[F, Stream[F, CommittableConsumerRecord[F, K, V]]] = {
        val chunkQueue: F[Queue[F, Option[Chunk[CommittableConsumerRecord[F, K, V]]]]] =
          Queue.bounded(settings.maxPrefetchBatches - 1)

        type PartitionRequest =
          (Chunk[CommittableConsumerRecord[F, K, V]], FetchCompletedReason)

        def enqueueStream(
          streamId: Int,
          partition: TopicPartition,
          partitions: Queue[F, Stream[F, CommittableConsumerRecord[F, K, V]]]
        ): F[Unit] = {
          for {
            chunks <- chunkQueue
            dequeueDone <- Deferred[F, Unit]
            shutdown = F.race(fiber.join.attempt, dequeueDone.get).void
            stopReqs <- Deferred.tryable[F, Unit]
            _ <- partitions.enqueue1 {
              Stream.eval {
                def fetchPartition(deferred: Deferred[F, PartitionRequest]): F[Unit] = {
                  val request = Request.Fetch(partition, streamId, deferred)
                  val fetch = requests.enqueue1(request) >> deferred.get
                  F.race(shutdown, fetch).flatMap {
                    case Left(()) =>
                      stopReqs.complete(())

                    case Right((chunk, reason)) =>
                      val enqueueChunk =
                        if (chunk.nonEmpty)
                          chunks.enqueue1(Some(chunk))
                        else F.unit

                      val completeRevoked =
                        if (reason.topicPartitionRevoked)
                          stopReqs.complete(())
                        else F.unit

                      enqueueChunk >> completeRevoked
                  }
                }

                F.guarantee {
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
                  }(F.race(dequeueDone.get, chunks.enqueue1(None)).void)
                  .start
                  .as {
                    chunks.dequeue.unNoneTerminate
                      .flatMap(Stream.chunk)
                      .covary[F]
                      .onFinalize(dequeueDone.complete(()))
                  }
              }.flatten
            }
          } yield ()
        }

        def enqueueStreams(
          streamId: Int,
          assigned: NonEmptySet[TopicPartition],
          partitions: Queue[F, Stream[F, CommittableConsumerRecord[F, K, V]]]
        ): F[Unit] = assigned.foldLeft(F.unit)(_ >> enqueueStream(streamId, _, partitions))

        def onRebalance(
          streamId: Int,
          partitions: Queue[F, Stream[F, CommittableConsumerRecord[F, K, V]]]
        ): OnRebalance[F, K, V] = OnRebalance(
          onAssigned = assigned =>
            NonEmptySet.fromSet(assigned).fold(F.unit)(enqueueStreams(streamId, _, partitions)),
          onRevoked = _ => F.unit
        )

        def requestAssignment(
          streamId: Int,
          partitions: Queue[F, Stream[F, CommittableConsumerRecord[F, K, V]]]
        ): F[SortedSet[TopicPartition]] = {
          Deferred[F, Either[Throwable, SortedSet[TopicPartition]]].flatMap { deferred =>
            val request =
              Request.Assignment[F, K, V](deferred, Some(onRebalance(streamId, partitions)))
            val assignment = requests.enqueue1(request) >> deferred.get.rethrow
            F.race(fiber.join.attempt, assignment).map {
              case Left(_)         => SortedSet.empty[TopicPartition]
              case Right(assigned) => assigned
            }
          }
        }

        def initialEnqueue(
          streamId: Int,
          partitions: Queue[F, Stream[F, CommittableConsumerRecord[F, K, V]]]
        ): F[Unit] =
          requestAssignment(streamId, partitions).flatMap { assigned =>
            if (assigned.nonEmpty) {
              val nonEmpty = NonEmptySet.fromSetUnsafe(assigned)
              enqueueStreams(streamId, nonEmpty, partitions)
            } else F.unit
          }

        val partitionQueue: F[Queue[F, Stream[F, CommittableConsumerRecord[F, K, V]]]] =
          Queue.unbounded[F, Stream[F, CommittableConsumerRecord[F, K, V]]]

        for {
          partitions <- Stream.eval(partitionQueue)
          streamId <- Stream.eval(streamIdRef.modify(n => (n + 1, n)))
          _ <- Stream.eval(initialEnqueue(streamId, partitions))
          out <- partitions.dequeue.interruptWhen(fiber.join.attempt)
        } yield out
      }

      override def stream: Stream[F, CommittableConsumerRecord[F, K, V]] =
        partitionedStream.parJoinUnbounded

      private[this] def request[A](
        request: Deferred[F, Either[Throwable, A]] => Request[F, K, V]
      ): F[A] =
        Deferred[F, Either[Throwable, A]].flatMap { deferred =>
          requests.enqueue1(request(deferred)) >>
            F.race(fiber.join, deferred.get.rethrow).flatMap {
              case Left(()) => F.raiseError(ConsumerShutdownException())
              case Right(a) => F.pure(a)
            }
        }

      override def assignment: F[SortedSet[TopicPartition]] = assignment(Option.empty)

      private def assignment(
        onRebalance: Option[OnRebalance[F, K, V]]
      ): F[SortedSet[TopicPartition]] =
        request(deferred => Request.Assignment(deferred = deferred, onRebalance = onRebalance))

      override def assignmentStream: Stream[F, SortedSet[TopicPartition]] =
        Stream.eval(Queue.unbounded[F, SortedSet[TopicPartition]]).flatMap { updateQueue =>
          val setup = for {
            currentAssignments <- assignment
            assignmentRef <- Ref.of(currentAssignments)
            onAssigned = (newPartitions: SortedSet[TopicPartition]) =>
              for {
                _ <- assignmentRef.update(_ ++ newPartitions)
                newAssignment <- assignmentRef.get
                _ <- updateQueue.enqueue1(newAssignment)
              } yield ()
            onRevoked = (revokedPartitions: SortedSet[TopicPartition]) =>
              for {
                _ <- assignmentRef.update(_ -- revokedPartitions)
                newAssignment <- assignmentRef.get
                _ <- updateQueue.enqueue1(newAssignment)
              } yield ()
            updated <- assignment(Some(OnRebalance(onAssigned, onRevoked)))
            _ <- updateQueue.enqueue1(updated)
          } yield ()

          Stream.eval(setup) >> updateQueue.dequeue.changes
        }

      override def seek(partition: TopicPartition, offset: Long): F[Unit] =
        withConsumer { consumer =>
          F.delay {
            consumer.seek(partition, offset)
          }
        }

      override def seekToBeginning: F[Unit] =
        seekToBeginning(List.empty[TopicPartition])

      override def seekToBeginning[G[_]](partitions: G[TopicPartition])(
        implicit G: Foldable[G]
      ): F[Unit] =
        withConsumer { consumer =>
          F.delay {
            consumer.seekToBeginning(partitions.asJava)
          }
        }

      override def seekToEnd: F[Unit] =
        seekToEnd(List.empty[TopicPartition])

      override def seekToEnd[G[_]](
        partitions: G[TopicPartition]
      )(implicit G: Foldable[G]): F[Unit] =
        withConsumer { consumer =>
          F.delay {
            consumer.seekToEnd(partitions.asJava)
          }
        }

      override def position(partition: TopicPartition): F[Long] =
        withConsumer { consumer =>
          F.delay {
            consumer.position(partition)
          }
        }

      override def position(partition: TopicPartition, timeout: FiniteDuration): F[Long] =
        withConsumer { consumer =>
          F.delay {
            consumer.position(partition, timeout.asJava)
          }
        }

      override def subscribeTo(firstTopic: String, remainingTopics: String*): F[Unit] =
        subscribe(NonEmptyList.of(firstTopic, remainingTopics: _*))

      override def subscribe[G[_]](topics: G[String])(implicit G: Reducible[G]): F[Unit] =
        request { deferred =>
          Request.SubscribeTopics(
            topics = topics.toNonEmptyList,
            deferred = deferred
          )
        }

      override def subscribe(regex: Regex): F[Unit] =
        request { deferred =>
          Request.SubscribePattern(
            pattern = regex.pattern,
            deferred = deferred
          )
        }

      override def beginningOffsets(
        partitions: Set[TopicPartition]
      ): F[Map[TopicPartition, Long]] =
        withConsumer { consumer =>
          F.delay {
            consumer
              .beginningOffsets(partitions.asJava)
              .asInstanceOf[util.Map[TopicPartition, Long]]
              .toMap
          }
        }

      override def beginningOffsets(
        partitions: Set[TopicPartition],
        timeout: FiniteDuration
      ): F[Map[TopicPartition, Long]] =
        withConsumer { consumer =>
          F.delay {
            consumer
              .beginningOffsets(partitions.asJava, timeout.asJava)
              .asInstanceOf[util.Map[TopicPartition, Long]]
              .toMap
          }
        }

      override def endOffsets(
        partitions: Set[TopicPartition]
      ): F[Map[TopicPartition, Long]] =
        withConsumer { consumer =>
          F.delay {
            consumer
              .endOffsets(partitions.asJava)
              .asInstanceOf[util.Map[TopicPartition, Long]]
              .toMap
          }
        }

      override def endOffsets(
        partitions: Set[TopicPartition],
        timeout: FiniteDuration
      ): F[Map[TopicPartition, Long]] =
        withConsumer { consumer =>
          F.delay {
            consumer
              .endOffsets(partitions.asJava, timeout.asJava)
              .asInstanceOf[util.Map[TopicPartition, Long]]
              .toMap
          }
        }

      override def toString: String =
        "KafkaConsumer$" + id
    }

  def consumerResource[F[_], K, V](
    settings: ConsumerSettings[F, K, V]
  )(
    implicit F: ConcurrentEffect[F],
    context: ContextShift[F],
    timer: Timer[F]
  ): Resource[F, KafkaConsumer[F, K, V]] =
    for {
      keyDeserializer <- Resource.liftF(settings.keyDeserializer)
      valueDeserializer <- Resource.liftF(settings.valueDeserializer)
      id <- Resource.liftF(F.delay(new Object().hashCode))
      implicit0(jitter: Jitter[F]) <- Resource.liftF(Jitter.default[F])
      implicit0(logging: Logging[F]) <- Resource.liftF(Logging.default[F](id))
      requests <- Resource.liftF(Queue.unbounded[F, Request[F, K, V]])
      polls <- Resource.liftF(Queue.bounded[F, Request[F, K, V]](1))
      ref <- Resource.liftF(Ref.of[F, State[F, K, V]](State.empty))
      streamId <- Resource.liftF(Ref.of[F, Int](0))
      withConsumer <- WithConsumer(settings)
      actor = new KafkaConsumerActor(
        settings = settings,
        keyDeserializer = keyDeserializer,
        valueDeserializer = valueDeserializer,
        ref = ref,
        requests = requests,
        withConsumer = withConsumer
      )
      actor <- startConsumerActor(requests, polls, actor)
      polls <- startPollScheduler(polls, settings.pollInterval)
    } yield createKafkaConsumer(requests, settings, actor, polls, streamId, id, withConsumer)
}

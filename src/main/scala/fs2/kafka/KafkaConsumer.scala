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

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.syntax.concurrent._
import cats.effect.{ConcurrentEffect, Fiber, Timer, _}
import cats.instances.list._
import cats.instances.unit._
import cats.syntax.applicativeError._
import cats.syntax.flatMap._
import cats.syntax.foldable._
import cats.syntax.functor._
import cats.syntax.monadError._
import cats.syntax.semigroup._
import cats.syntax.traverse._
import fs2.concurrent.Queue
import fs2.kafka.KafkaConsumerActor.Request._
import fs2.kafka.KafkaConsumerActor._
import fs2.kafka.internal.Synchronized
import fs2.kafka.internal.instances._
import fs2.kafka.internal.syntax._
import fs2.{Chunk, Stream}
import org.apache.kafka.clients.consumer.{KafkaConsumer => KConsumer, _}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.collection.immutable.SortedSet
import scala.concurrent.duration.FiniteDuration

abstract class KafkaConsumer[F[_], K, V] {
  def stream: Stream[F, CommittableMessage[F, K, V]]

  def partitionedStream: Stream[F, Stream[F, CommittableMessage[F, K, V]]]

  def parallelPartitionedStream: Stream[F, Stream[F, CommittableMessage[F, K, V]]]

  def subscribe(topics: NonEmptyList[String]): Stream[F, Unit]

  /**
    * A `Fiber` that can be used to cancel the underlying consumer,
    * or wait for it to complete. If you're using [[stream]] or other
    * provided streams, like [[partitionedStream]], these streams are
    * automatically interrupted when the underlying consumer has been
    * cancelled or when it finishes with an exception.<br>
    * <br>
    * When `cancel` is invoked, an attempt will be made to stop the
    * underlying consumer. Note that `cancel` does not wait for the
    * consumer to stop. If you also want to wait for the consumer
    * to stop, you can use `join`.
    */
  def fiber: Fiber[F, Unit]
}

private[kafka] object KafkaConsumer {
  private[this] def createConsumer[F[_], K, V](
    settings: ConsumerSettings[K, V]
  )(
    implicit F: Concurrent[F],
    context: ContextShift[F]
  ): Resource[F, Synchronized[F, Consumer[K, V]]] =
    Resource.make[F, Synchronized[F, Consumer[K, V]]] {
      F.delay {
          new KConsumer(
            (settings.properties: Map[String, AnyRef]).asJava,
            settings.keyDeserializer,
            settings.valueDeserializer
          )
        }
        .flatMap(Synchronized[F].of)
    } { synchronized =>
      synchronized.use { consumer =>
        context.evalOn(settings.executionContext) {
          F.delay(consumer.close(settings.closeTimeout.asJava))
        }
      }
    }

  private[this] def startConsumerActor[F[_], K, V](
    requests: Queue[F, Request[F, K, V]],
    polls: Queue[F, Request[F, K, V]],
    actor: KafkaConsumerActor[F, K, V]
  )(
    implicit F: Concurrent[F],
    context: ContextShift[F]
  ): Resource[F, Fiber[F, Unit]] =
    Resource.make {
      Deferred[F, Unit].flatMap { deferred =>
        F.guarantee {
            requests.tryDequeue1
              .flatMap(_.map(F.pure).getOrElse(polls.dequeue1))
              .flatMap(actor.handle(_) >> context.shift)
              .foreverM[Unit]
          }(deferred.complete(()))
          .start
          .map(fiber => Fiber[F, Unit](deferred.get, fiber.cancel.start.void))
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
      Deferred[F, Unit].flatMap { deferred =>
        F.guarantee {
            {
              polls.enqueue1(Poll()) >>
                timer.sleep(pollInterval)
            }.foreverM[Unit]
          }(deferred.complete(()))
          .start
          .map(fiber => Fiber[F, Unit](deferred.get, fiber.cancel.start.void))
      }
    }(_.cancel)

  private[this] def createKafkaConsumer[F[_], K, V](
    requests: Queue[F, Request[F, K, V]],
    actor: Fiber[F, Unit],
    polls: Fiber[F, Unit]
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

      override val parallelPartitionedStream: Stream[F, Stream[F, CommittableMessage[F, K, V]]] = {
        val chunkQueue: F[Queue[F, Option[Chunk[CommittableMessage[F, K, V]]]]] =
          Queue.bounded[F, Option[Chunk[CommittableMessage[F, K, V]]]](1)

        type PartitionRequest =
          (Chunk[CommittableMessage[F, K, V]], FetchCompletedReason)

        def enqueueStream(
          partition: TopicPartition,
          partitions: Queue[F, Stream[F, CommittableMessage[F, K, V]]]
        ): F[Unit] =
          chunkQueue.flatMap { chunks =>
            Deferred[F, Unit].flatMap { dequeueDone =>
              Deferred[F, Unit].flatMap { partitionRevoked =>
                val shutdown = F.race(fiber.join, dequeueDone.get).void
                partitions.enqueue1 {
                  Stream.eval {
                    F.guarantee {
                        Stream
                          .repeatEval {
                            Deferred[F, PartitionRequest].flatMap { deferred =>
                              val request = Request.Fetch(partition, deferred)
                              val fetch = requests.enqueue1(request) >> deferred.get
                              F.race(shutdown, fetch).flatMap {
                                case Left(()) => F.unit
                                case Right((chunk, reason)) =>
                                  val enqueueChunk =
                                    if (chunk.nonEmpty)
                                      chunks.enqueue1(Some(chunk))
                                    else F.unit

                                  val completeRevoked =
                                    if (reason.topicPartitionRevoked)
                                      partitionRevoked.complete(())
                                    else F.unit

                                  enqueueChunk >> completeRevoked
                              }
                            }
                          }
                          .interruptWhen(F.race(shutdown, partitionRevoked.get).void.attempt)
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
              }
            }
          }

        def enqueueStreams(
          assigned: NonEmptySet[TopicPartition],
          partitions: Queue[F, Stream[F, CommittableMessage[F, K, V]]]
        ): F[Unit] = assigned.foldLeft(F.unit)(_ >> enqueueStream(_, partitions))

        def onRebalance(
          partitions: Queue[F, Stream[F, CommittableMessage[F, K, V]]]
        ): OnRebalance[F, K, V] = OnRebalance(
          onAssigned = assigned => enqueueStreams(assigned.partitions, partitions),
          onRevoked = _ => F.unit
        )

        def requestAssignment(
          partitions: Queue[F, Stream[F, CommittableMessage[F, K, V]]]
        ): F[SortedSet[TopicPartition]] = {
          Deferred[F, Either[Throwable, SortedSet[TopicPartition]]].flatMap { deferred =>
            val request = Request.Assignment[F, K, V](deferred, Some(onRebalance(partitions)))
            val assignment = requests.enqueue1(request) >> deferred.get.rethrow
            F.race(fiber.join, assignment).map {
              case Left(())        => SortedSet.empty[TopicPartition]
              case Right(assigned) => assigned
            }
          }
        }

        def initialEnqueue(partitions: Queue[F, Stream[F, CommittableMessage[F, K, V]]]): F[Unit] =
          requestAssignment(partitions).flatMap { assigned =>
            if (assigned.nonEmpty) {
              val nonEmpty = NonEmptySet.fromSetUnsafe(assigned)
              enqueueStreams(nonEmpty, partitions)
            } else F.unit
          }

        val partitionQueue: F[Queue[F, Stream[F, CommittableMessage[F, K, V]]]] =
          Queue.unbounded[F, Stream[F, CommittableMessage[F, K, V]]]

        Stream.eval(partitionQueue).flatMap { partitions =>
          Stream.eval(initialEnqueue(partitions)) >>
            partitions.dequeue.interruptWhen(fiber.join.attempt)
        }
      }

      override val partitionedStream: Stream[F, Stream[F, CommittableMessage[F, K, V]]] = {
        val requestAssignment: F[SortedSet[TopicPartition]] =
          Deferred[F, Either[Throwable, SortedSet[TopicPartition]]].flatMap { deferred =>
            val request = Request.Assignment[F, K, V](deferred, onRebalance = None)
            val assignment = requests.enqueue1(request) >> deferred.get.rethrow
            F.race(fiber.join, assignment).map {
              case Left(())        => SortedSet.empty[TopicPartition]
              case Right(assigned) => assigned
            }
          }

        type PartitionRequest =
          (Chunk[CommittableMessage[F, K, V]], ExpiringFetchCompletedReason)

        def chunkQueue(size: Int): F[Queue[F, Option[Chunk[CommittableMessage[F, K, V]]]]] =
          Queue.bounded[F, Option[Chunk[CommittableMessage[F, K, V]]]](size)

        def requestPartitions(
          assigned: SortedSet[TopicPartition]
        ): F[Stream[F, CommittableMessage[F, K, V]]] =
          chunkQueue(assigned.size).flatMap { chunks =>
            Deferred[F, Unit].flatMap { dequeueDone =>
              F.guarantee {
                  assigned.toList
                    .traverse { partition =>
                      Deferred[F, PartitionRequest].flatMap { deferred =>
                        val request = Request.ExpiringFetch(partition, deferred)
                        val fetch = requests.enqueue1(request) >> deferred.get
                        F.race(F.race(fiber.join, dequeueDone.get), fetch).flatMap {
                          case Right((chunk, _)) if chunk.nonEmpty =>
                            chunks.enqueue1(Some(chunk))
                          case _ => F.unit
                        }
                      }.start
                    }
                    .flatMap(_.combineAll.join)
                }(F.race(dequeueDone.get, chunks.enqueue1(None)).void)
                .start
                .as {
                  chunks.dequeue.unNoneTerminate
                    .flatMap(Stream.chunk)
                    .covary[F]
                    .onFinalize(dequeueDone.complete(()))
                }
            }
          }

        Stream
          .repeatEval(requestAssignment)
          .filter(_.nonEmpty)
          .evalMap(requestPartitions)
          .interruptWhen(fiber.join.attempt)
      }

      override val stream: Stream[F, CommittableMessage[F, K, V]] =
        partitionedStream.flatten

      override def subscribe(topics: NonEmptyList[String]): Stream[F, Unit] =
        Stream.eval(requests.enqueue1(Subscribe(topics)))

      override def toString: String =
        "KafkaConsumer$" + System.identityHashCode(this)
    }

  def consumerResource[F[_], K, V](
    settings: ConsumerSettings[K, V]
  )(
    implicit F: ConcurrentEffect[F],
    context: ContextShift[F],
    timer: Timer[F]
  ): Resource[F, KafkaConsumer[F, K, V]] =
    Resource.liftF(Queue.unbounded[F, Request[F, K, V]]).flatMap { requests =>
      Resource.liftF(Queue.bounded[F, Request[F, K, V]](1)).flatMap { polls =>
        Resource.liftF(Ref.of[F, State[F, K, V]](State.empty)).flatMap { ref =>
          Resource.liftF(Jitter.default[F]).flatMap { implicit jitter =>
            createConsumer(settings).flatMap { synchronized =>
              val actor = new KafkaConsumerActor(settings, ref, requests, synchronized)
              startConsumerActor(requests, polls, actor).flatMap { actor =>
                startPollScheduler(polls, settings.pollInterval).map { polls =>
                  createKafkaConsumer(requests, actor, polls)
                }
              }
            }
          }
        }
      }
    }
}

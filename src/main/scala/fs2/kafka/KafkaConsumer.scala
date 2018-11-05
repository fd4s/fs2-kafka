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

import cats.data.NonEmptyList
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
import fs2.kafka.internal.syntax._
import fs2.{Chunk, Stream}
import org.apache.kafka.clients.consumer.{KafkaConsumer => KConsumer, _}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

abstract class KafkaConsumer[F[_], K, V] {
  def stream: Stream[F, CommittableMessage[F, K, V]]

  def partitionedStream: Stream[F, Stream[F, CommittableMessage[F, K, V]]]

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

      private val assignment: F[Set[TopicPartition]] =
        Deferred[F, Either[Throwable, Set[TopicPartition]]].flatMap { deferred =>
          val assignment = requests.enqueue1(Assignment(deferred)) >> deferred.get.rethrow
          F.race(fiber.join, assignment).map {
            case Left(())        => Set.empty
            case Right(assigned) => assigned
          }
        }

      private def requestPartitions(
        assigned: Set[TopicPartition]
      ): F[Stream[F, CommittableMessage[F, K, V]]] =
        Queue
          .bounded[F, Option[Chunk[CommittableMessage[F, K, V]]]](assigned.size)
          .flatMap { queue =>
            assigned.toList
              .traverse { partition =>
                Deferred[F, Chunk[CommittableMessage[F, K, V]]].flatMap { deferred =>
                  val fetch = requests.enqueue1(Fetch(partition, deferred)) >> deferred.get
                  F.race(fiber.join, fetch).flatMap {
                    case Left(()) => F.unit
                    case Right(chunk) =>
                      if (chunk.nonEmpty)
                        queue.enqueue1(Some(chunk))
                      else F.unit
                  }
                }.start
              }
              .flatMap(_.combineAll.join)
              .flatMap(_ => queue.enqueue1(None))
              .start
              .as {
                queue.dequeue.unNoneTerminate
                  .flatMap(Stream.chunk)
                  .covary[F]
              }
          }

      private val empty: F[Stream[F, CommittableMessage[F, K, V]]] =
        F.pure(Stream.empty.covaryAll[F, CommittableMessage[F, K, V]])

      override val partitionedStream: Stream[F, Stream[F, CommittableMessage[F, K, V]]] =
        Stream
          .repeatEval {
            assignment.flatMap { assigned =>
              if (assigned.isEmpty) empty
              else requestPartitions(assigned)
            }
          }
          .interruptWhen(fiber.join.attempt)

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

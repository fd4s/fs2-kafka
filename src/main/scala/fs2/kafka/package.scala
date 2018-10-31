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

package fs2

import java.util.concurrent.{Executors, ThreadFactory}

import cats.effect._
import cats.syntax.functor._

import scala.concurrent.ExecutionContext

package object kafka {
  def consumerResource[F[_], K, V](settings: ConsumerSettings[K, V])(
    implicit F: ConcurrentEffect[F],
    context: ContextShift[F],
    timer: Timer[F]
  ): Resource[F, KafkaConsumer[F, K, V]] =
    KafkaConsumer.consumerResource(settings)

  def consumerResource[F[_]]: ConsumerResource[F] =
    new ConsumerResource[F]

  def consumerStream[F[_], K, V](settings: ConsumerSettings[K, V])(
    implicit F: ConcurrentEffect[F],
    context: ContextShift[F],
    timer: Timer[F]
  ): Stream[F, KafkaConsumer[F, K, V]] =
    Stream.resource(consumerResource(settings))

  def consumerStream[F[_]]: ConsumerStream[F] =
    new ConsumerStream[F]

  def consumerExecutionContextResource[F[_]](implicit F: Sync[F]): Resource[F, ExecutionContext] =
    Resource
      .make {
        F.delay {
          Executors.newSingleThreadExecutor(new ThreadFactory {
            override def newThread(runnable: Runnable): Thread = {
              val thread = new Thread(runnable)
              thread.setName(s"fs2-kafka-consumer-${thread.getId}")
              thread.setDaemon(true)
              thread
            }
          })
        }
      }(executor => F.delay(executor.shutdown()))
      .map(ExecutionContext.fromExecutor)

  def consumerExecutionContextStream[F[_]](implicit F: Sync[F]): Stream[F, ExecutionContext] =
    Stream.resource(consumerExecutionContextResource[F])

  def producerResource[F[_], K, V](settings: ProducerSettings[K, V])(
    implicit F: ConcurrentEffect[F]
  ): Resource[F, KafkaProducer[F, K, V]] =
    KafkaProducer.producerResource(settings)

  def producerResource[F[_]]: ProducerResource[F] =
    new ProducerResource[F]

  def producerStream[F[_], K, V](settings: ProducerSettings[K, V])(
    implicit F: ConcurrentEffect[F]
  ): Stream[F, KafkaProducer[F, K, V]] =
    Stream.resource(producerResource(settings))

  def producerStream[F[_]]: ProducerStream[F] =
    new ProducerStream[F]
}

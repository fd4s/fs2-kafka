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

import cats.effect.{Blocker, Resource, Sync}
import java.util.concurrent.{ExecutorService, Executors, ThreadFactory}

import scala.concurrent.ExecutionContext

private[kafka] object Blockers {
  def consumer[F[_]](implicit F: Sync[F]): Resource[F, Blocker] =
    singleThreadBlocker("fs2-kafka-consumer")

  def producer[F[_]](implicit F: Sync[F]): Resource[F, Blocker] =
    singleThreadBlocker("fs2-kafka-producer")

  def transactionalProducer[F[_]](implicit F: Sync[F]): Resource[F, Blocker] =
    cachingBlocker("fs2-kafka-transactional-producer")

  def adminClient[F[_]](implicit F: Sync[F]): Resource[F, Blocker] =
    singleThreadBlocker("fs2-kafka-admin-client")

  private[this] def singleThreadBlocker[F[_]](
    name: String
  )(implicit F: Sync[F]): Resource[F, Blocker] =
    blocker(F.delay(Executors.newSingleThreadExecutor(threadFactory(name))))

  private[this] def cachingBlocker[F[_]](name: String)(implicit F: Sync[F]): Resource[F, Blocker] =
    blocker(F.delay(Executors.newCachedThreadPool(threadFactory(name))))

  private[this] def threadFactory(name: String): ThreadFactory =
    new ThreadFactory {
      override def newThread(runnable: Runnable): Thread = {
        val thread = new Thread(runnable)
        thread.setName(s"$name-${thread.getId}")
        thread.setDaemon(true)
        thread
      }
    }

  private[this] def blocker[F[_]](
    executor: F[ExecutorService]
  )(implicit F: Sync[F]): Resource[F, Blocker] =
    Resource.make(executor)(es => F.delay(es.shutdown())).map { es =>
      Blocker.liftExecutionContext(ExecutionContext.fromExecutor(es))
    }
}

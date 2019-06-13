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
import java.util.concurrent.{Executors, ThreadFactory}
import scala.concurrent.ExecutionContext

private[kafka] object Blockers {
  def consumer[F[_]](implicit F: Sync[F]): Resource[F, Blocker] =
    blocker("fs2-kafka-consumer")

  def producer[F[_]](implicit F: Sync[F]): Resource[F, Blocker] =
    blocker("fs2-kafka-producer")

  def adminClient[F[_]](implicit F: Sync[F]): Resource[F, Blocker] =
    blocker("fs2-kafka-admin-client")

  private[this] def blocker[F[_]](name: String)(implicit F: Sync[F]): Resource[F, Blocker] =
    Resource {
      F.delay {
        val executor =
          Executors.newSingleThreadExecutor(
            new ThreadFactory {
              override def newThread(runnable: Runnable): Thread = {
                val thread = new Thread(runnable)
                thread.setName(s"$name-${thread.getId}")
                thread.setDaemon(true)
                thread
              }
            }
          )

        val blocker =
          Blocker.liftExecutionContext {
            ExecutionContext.fromExecutor(executor)
          }

        val shutdown =
          F.delay(executor.shutdown())

        (blocker, shutdown)
      }
    }
}

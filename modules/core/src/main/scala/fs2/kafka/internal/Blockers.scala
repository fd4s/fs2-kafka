/*
 * Copyright 2018-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
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

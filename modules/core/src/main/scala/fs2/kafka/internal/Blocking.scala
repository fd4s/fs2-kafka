/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.effect.{Async, Resource, Sync}

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import cats.effect.syntax.async._

private[kafka] trait Blocking[F[_]] {
  def apply[A](a: => A): F[A]
}

private[kafka] object Blocking {
  def apply[F[_]: Sync]: Blocking[F] = new Blocking[F] {
    override def apply[A](a: => A): F[A] = Sync[F].blocking(a)
  }

  def singleThreaded[F[_]](name: String)(implicit F: Async[F]): Resource[F, Blocking[F]] =
    Resource {
      F.delay {
        val executor =
          Executors.newSingleThreadExecutor(
            (runnable: Runnable) => {
              val thread = new Thread(runnable)
              thread.setName(s"$name-${thread.getId}")
              thread.setDaemon(true)
              thread
            }
          )

        val ec = ExecutionContext.fromExecutor(executor)

        val blocking: Blocking[F] = new Blocking[F] {
          def apply[A](a: => A): F[A] = F.delay(a).evalOn(ec)
        }

        val shutdown =
          F.delay(executor.shutdown())

        (blocking, shutdown)
      }
    }
}

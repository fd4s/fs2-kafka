/*
 * Copyright 2018-2020 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.effect.{Blocker, Concurrent, ContextShift, Resource}
import cats.effect.concurrent.Semaphore
import cats.implicits._
import fs2.kafka.{KafkaByteConsumer, ConsumerSettings}
import fs2.kafka.internal.syntax._

private[kafka] sealed abstract class WithConsumer[F[_]] {
  def blocking[A](f: KafkaByteConsumer => A): F[A]
}

private[kafka] object WithConsumer {
  def apply[F[_], K, V](
    settings: ConsumerSettings[F, K, V]
  )(
    implicit F: Concurrent[F],
    context: ContextShift[F]
  ): Resource[F, WithConsumer[F]] = {
    val blockerResource =
      settings.blocker
        .map(Resource.pure[F, Blocker])
        .getOrElse(Blockers.consumer)

    blockerResource.flatMap { blocker =>
      Resource.make {
        (settings.createConsumer, Semaphore[F](1L))
          .mapN { (consumer, semaphore) =>
            new WithConsumer[F] {
              override def blocking[A](f: KafkaByteConsumer => A): F[A] =
                semaphore.withPermit {
                  blocker.delay(f(consumer))
                }
            }
          }
      }(_.blocking { _.close(settings.closeTimeout.asJava) })
    }
  }
}

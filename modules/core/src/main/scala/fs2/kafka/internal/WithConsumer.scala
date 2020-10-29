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
  def apply[A](f: (KafkaByteConsumer, Blocking[F]) => F[A]): F[A]

  def blocking[A](f: KafkaByteConsumer => A): F[A] = apply {
    case (producer, blocking) => blocking(f(producer))
  }
}

private[kafka] object WithConsumer {
  def apply[F[_], K, V](
    settings: ConsumerSettings[F, K, V]
  )(
    implicit F: Concurrent[F],
    context: ContextShift[F]
  ): Resource[F, WithConsumer[F]] = {
    val blockingResource =
      settings.blocker
        .map(Resource.pure[F, Blocker])
        .getOrElse(Blockers.consumer)
        .map(Blocking.fromBlocker[F])

    blockingResource.flatMap { blocking_ =>
      Resource.make {
        (settings.createConsumer, Semaphore[F](1L))
          .mapN { (consumer, semaphore) =>
            new WithConsumer[F] {
              override def apply[A](f: (KafkaByteConsumer, Blocking[F]) => F[A]): F[A] =
                semaphore.withPermit { f(consumer, blocking_) }
            }
          }
      }(_.blocking { _.close(settings.closeTimeout.asJava) })
    }
  }
}

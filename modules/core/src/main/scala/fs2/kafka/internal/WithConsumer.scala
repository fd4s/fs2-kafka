/*
 * Copyright 2018-2020 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.effect.{Resource, Async}
import cats.effect.std.Semaphore
import cats.implicits._
import fs2.kafka.{KafkaByteConsumer, ConsumerSettings}
import fs2.kafka.internal.syntax._

private[kafka] sealed abstract class WithConsumer[F[_]] {
  def apply[A](f: KafkaByteConsumer => F[A]): F[A]
  def blocking[A](f: KafkaByteConsumer => A): F[A]
}

private[kafka] object WithConsumer {
  def apply[F[_], K, V](
    settings: ConsumerSettings[F, K, V]
  )(
    implicit F: Async[F]
  ): Resource[F, WithConsumer[F]] = {

    Resource[F, WithConsumer[F]] {
      (settings.createConsumer, Semaphore[F](1))
        .mapN { (consumer, semaphore) =>
          val withConsumer =
            new WithConsumer[F] {
              override def apply[A](f: KafkaByteConsumer => F[A]): F[A] =
                semaphore.permit.use { _ =>
                  F.defer(f(consumer))
                }

              override def blocking[A](f: KafkaByteConsumer => A): F[A] =
                apply(a => F.blocking(f(a)))
            }

          val close =
            withConsumer.blocking {
              _.close(settings.closeTimeout.asJava)
            }

          (withConsumer, close)
        }
    }
  }
}

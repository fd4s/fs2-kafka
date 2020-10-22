/*
 * Copyright 2018-2020 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.effect.{Concurrent, Resource, Sync, Async}
import cats.implicits._
import fs2.kafka.{KafkaByteConsumer, ConsumerSettings}
import fs2.kafka.internal.syntax._

private[kafka] sealed abstract class WithConsumer[F[_]] {
  def apply[A](f: KafkaByteConsumer => F[A]): F[A]
}

private[kafka] object WithConsumer {
  def apply[F[_], K, V](
    settings: ConsumerSettings[F, K, V]
  )(
    implicit F: Async[F]
  ): Resource[F, WithConsumer[F]] = {

    Resource[F, WithConsumer[F]] {
      settings.createConsumer
        .flatMap(Synchronized[F].of)
        .map { synchronizedConsumer =>
          val withConsumer =
            new WithConsumer[F] {
              override def apply[A](f: KafkaByteConsumer => F[A]): F[A] =
                synchronizedConsumer.use { consumer =>
                  F.defer(f(consumer))
                }
            }

          val close =
            withConsumer { consumer =>
              F.delay(consumer.close(settings.closeTimeout.asJava))
            }

          (withConsumer, close)
        }
    }
  }
}

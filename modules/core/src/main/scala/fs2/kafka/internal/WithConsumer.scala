/*
 * Copyright 2018-2019 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.effect.{Blocker, Concurrent, ContextShift, Resource}
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
    implicit F: Concurrent[F],
    context: ContextShift[F]
  ): Resource[F, WithConsumer[F]] = {
    val blockerResource =
      settings.blocker
        .map(Resource.pure[F, Blocker])
        .getOrElse(Blockers.consumer)

    blockerResource.flatMap { blocker =>
      Resource[F, WithConsumer[F]] {
        settings.createConsumer
          .flatMap(Synchronized[F].of)
          .map { synchronizedConsumer =>
            val withConsumer =
              new WithConsumer[F] {
                override def apply[A](f: KafkaByteConsumer => F[A]): F[A] =
                  synchronizedConsumer.use { consumer =>
                    context.blockOn(blocker) {
                      f(consumer)
                    }
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
}

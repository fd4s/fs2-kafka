/*
 * Copyright 2018-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.effect.{Blocker, Concurrent, ContextShift, Resource}
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
    val blockingResource =
      settings.blocker
        .map(Resource.pure[F, Blocker])
        .getOrElse(Blockers.consumer)
        .map(Blocking.fromBlocker[F])

    blockingResource.flatMap { blocking_ =>
      Resource.make {
        settings.createConsumer
          .map { consumer =>
            new WithConsumer[F] {
              override def blocking[A](f: KafkaByteConsumer => A): F[A] =
                blocking_(f(consumer))
            }
          }
      }(_.blocking { _.close(settings.closeTimeout.asJava) })
    }
  }
}

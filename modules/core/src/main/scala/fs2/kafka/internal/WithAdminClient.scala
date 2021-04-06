/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.effect.{Async, Resource}
import cats.implicits._
import fs2.kafka.AdminClientSettings
import fs2.kafka.admin.MkAdminClient
import fs2.kafka.internal.syntax._
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.KafkaFuture

private[kafka] sealed abstract class WithAdminClient[F[_]] {
  def apply[A](f: AdminClient => KafkaFuture[A]): F[A]
}

private[kafka] object WithAdminClient {
  def apply[F[_]](
    mk: MkAdminClient[F],
    settings: AdminClientSettings
  )(implicit F: Async[F]): Resource[F, WithAdminClient[F]] =
    Resource[F, WithAdminClient[F]] {
      mk(settings).map { adminClient =>
        val withAdminClient =
          new WithAdminClient[F] {
            override def apply[A](f: AdminClient => KafkaFuture[A]): F[A] =
              F.defer(f(adminClient).cancelable)
          }

        val close =
          F.blocking(adminClient.close(settings.closeTimeout.asJava))

        (withAdminClient, close)
      }
    }
}

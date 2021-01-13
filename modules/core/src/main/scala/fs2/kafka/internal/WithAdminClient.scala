/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.effect.{Blocker, Concurrent, ContextShift, Resource}
import cats.implicits._
import fs2.kafka.AdminClientSettings
import fs2.kafka.internal.syntax._
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.KafkaFuture

private[kafka] sealed abstract class WithAdminClient[F[_]] {
  def apply[A](f: AdminClient => KafkaFuture[A]): F[A]
}

private[kafka] object WithAdminClient {
  def apply[F[_]](
    settings: AdminClientSettings[F]
  )(
    implicit F: Concurrent[F],
    context: ContextShift[F]
  ): Resource[F, WithAdminClient[F]] = {
    val blockerResource =
      settings.blocker
        .map(Resource.pure[F, Blocker])
        .getOrElse(Blockers.adminClient)

    blockerResource.flatMap { blocker =>
      Resource[F, WithAdminClient[F]] {
        settings.createAdminClient.map { adminClient =>
          val withAdminClient =
            new WithAdminClient[F] {
              override def apply[A](f: AdminClient => KafkaFuture[A]): F[A] =
                context.blockOn(blocker) {
                  F.suspend(f(adminClient).cancelable)
                }
            }

          val close =
            context.blockOn(blocker) {
              F.delay(adminClient.close(settings.closeTimeout.asJava))
            }

          (withAdminClient, close)
        }
      }
    }
  }
}

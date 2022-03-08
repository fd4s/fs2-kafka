/*
 * Copyright 2018-2022 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.{Blocker, ContextShift, Resource, Sync}

private[kafka] package object internal {
  private[kafka] type ExclusiveAccess[F[_], A] = F[A] => F[A]

  private[kafka] def blockingResource[F[_]: Sync: ContextShift](
    settings: ProducerSettings[F, _, _]
  ): Resource[F, Blocking[F]] =
    settings.blocker
      .map(Resource.pure[F, Blocker])
      .getOrElse(Blockers.producer)
      .map(Blocking.fromBlocker[F])
}

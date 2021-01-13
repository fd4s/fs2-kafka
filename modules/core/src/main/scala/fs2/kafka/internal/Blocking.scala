/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.effect.{Blocker, ContextShift, Sync}

private[kafka] trait Blocking[F[_]] {
  def apply[A](a: => A): F[A]
}

private[kafka] object Blocking {
  def fromBlocker[F[_]: Sync: ContextShift](blocker: Blocker): Blocking[F] = new Blocking[F] {
    override def apply[A](a: => A): F[A] = blocker.delay(a)
  }
}

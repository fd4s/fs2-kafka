/*
 * Copyright 2018-2022 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.effect.Sync

private[kafka] trait Blocking[F[_]] {
  def apply[A](a: => A): F[A]
}

private[kafka] object Blocking {
  def fromBlocker[F[_]: Sync: ContextShift]: Blocking[F] = new Blocking[F] {
    override def apply[A](a: => A): F[A] = Sync[F].blocking(a)
  }
}

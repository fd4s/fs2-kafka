/*
 * Copyright 2018-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.effect.{Concurrent, Outcome}
import cats.syntax.all._
import cats.effect.syntax.all._

/** A wrapper for `cancel` and `join` effects used to terminate and await
  *  termination of running processes, ported from `Fiber` in cats-effect 2.
  */
private[kafka] final case class FakeFiber[F[_]](join: F[Unit], cancel: F[Unit])(
  implicit F: Concurrent[F]
) {
  def combine(that: FakeFiber[F]): FakeFiber[F] = {
    val fa0join =
      this.join.guaranteeCase {
        case Outcome.Canceled() => F.unit
        case _                  => that.cancel
      }

    val fb0join =
      that.join.guaranteeCase {
        case Outcome.Canceled() => F.unit
        case _                  => this.cancel
      }

    FakeFiber(
      F.racePair(fa0join, fb0join).flatMap {
        case Left((a, fiberB))  => F.map2(a.embedNever, fiberB.joinWithNever)((_, _) => ())
        case Right((fiberA, b)) => F.map2(fiberA.joinWithNever, b.embedNever)((_, _) => ())
      },
      F.map2(this.cancel, that.cancel)((_, _) => ())
    )
  }
}

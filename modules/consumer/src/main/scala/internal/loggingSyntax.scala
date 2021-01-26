/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.FlatMap
import cats.syntax.all._

private[kafka] object loggingSyntax {
  implicit final class LoggingSyntax[F[_], A](
    private val fa: F[A]
  ) extends AnyVal {
    def log(f: A => LogEntry)(
      implicit F: FlatMap[F],
      logging: Logging[F]
    ): F[Unit] =
      fa.flatMap(a => logging.log(f(a)))
  }
}

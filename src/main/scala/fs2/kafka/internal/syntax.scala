/*
 * Copyright 2018 OVO Energy Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fs2.kafka.internal

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit

import cats.effect.Bracket
import cats.effect.concurrent.MVar

import scala.concurrent.duration.FiniteDuration

private[kafka] object syntax {
  implicit final class FiniteDurationSyntax(val duration: FiniteDuration) extends AnyVal {
    def asJava: Duration =
      if (duration.length == 0L) Duration.ZERO
      else
        duration.unit match {
          case TimeUnit.DAYS         => Duration.ofDays(duration.length)
          case TimeUnit.HOURS        => Duration.ofHours(duration.length)
          case TimeUnit.MINUTES      => Duration.ofMinutes(duration.length)
          case TimeUnit.SECONDS      => Duration.ofSeconds(duration.length)
          case TimeUnit.MILLISECONDS => Duration.ofMillis(duration.length)
          case TimeUnit.MICROSECONDS => Duration.of(duration.length, ChronoUnit.MICROS)
          case TimeUnit.NANOSECONDS  => Duration.ofNanos(duration.length)
        }
  }

  implicit final class MVarSyntax[F[_], A](val mVar: MVar[F, A]) extends AnyVal {
    def lease[B](f: A => F[B])(implicit F: Bracket[F, Throwable]): F[B] =
      F.bracket(mVar.take)(f)(mVar.put)
  }
}

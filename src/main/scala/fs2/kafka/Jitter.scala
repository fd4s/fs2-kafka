/*
 * Copyright 2018-2019 OVO Energy Limited
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

package fs2.kafka

import cats.Applicative
import cats.effect.Sync
import cats.syntax.functor._

/**
  * [[Jitter]] represents the ability to apply jitter to an existing value
  * `n`, effectively multiplying `n` with a pseudorandom value between `0`
  * and `1` (both inclusive, although implementation dependent).<br>
  * <br>
  * The default [[Jitter#default]] uses `java.util.Random` for pseudorandom
  * values and always applies jitter with a value between `0` (inclusive)
  * and `1` (exclusive). If no jitter is desired, use [[Jitter#none]].
  */
sealed abstract class Jitter[F[_]] {
  def withJitter(n: Double): F[Double]
}

object Jitter {
  def apply[F[_]](implicit F: Jitter[F]): Jitter[F] = F

  /**
    * Creates a default [[Jitter]] instance, which uses `java.util.Random`
    * for generating pseudorandom values, always applying jitter with a
    * value between `0` (inclusive) and `1` (exclusive).
    */
  def default[F[_]](implicit F: Sync[F]): F[Jitter[F]] =
    F.delay(new java.util.Random()).map { random =>
      new Jitter[F] {
        override def withJitter(n: Double): F[Double] =
          F.delay(random.nextDouble()).map(_ * n)
      }
    }

  /**
    * Creates a [[Jitter]] instance which does not apply jitter,
    * meaning all input values will be returned unmodified.
    */
  def none[F[_]](implicit F: Applicative[F]): Jitter[F] =
    new Jitter[F] {
      override def withJitter(n: Double): F[Double] =
        F.pure(n)
    }
}

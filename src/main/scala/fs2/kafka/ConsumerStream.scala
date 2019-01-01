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

import cats.effect.{ConcurrentEffect, ContextShift, Timer}
import fs2.Stream

/**
  * [[ConsumerStream]] provides support for inferring the key and value
  * type from [[ConsumerSettings]] when using `consumerStream` with the
  * following syntax.
  *
  * {{{
  * consumerStream[F].using(settings)
  * }}}
  */
final class ConsumerStream[F[_]] private[kafka] (
  private val F: ConcurrentEffect[F]
) extends AnyVal {

  /**
    * Creates a new [[KafkaConsumer]] in the `Stream` context.
    * This is equivalent to using `consumerStream` directly,
    * except we're able to infer the key and value type.
    */
  def using[K, V](settings: ConsumerSettings[K, V])(
    implicit context: ContextShift[F],
    timer: Timer[F]
  ): Stream[F, KafkaConsumer[F, K, V]] =
    consumerStream(settings)(F, context, timer)

  override def toString: String =
    "ConsumerStream$" + System.identityHashCode(this)
}

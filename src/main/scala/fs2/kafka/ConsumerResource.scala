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

package fs2.kafka

import cats.effect.{ConcurrentEffect, ContextShift, Resource, Timer}

/**
  * [[ConsumerResource]] provides support for inferring the key and
  * value type from [[ConsumerSettings]] when using `consumerResource`
  * using the following syntax.
  *
  * {{{
  * consumerResource[F].using(settings)
  * }}}
  */
final class ConsumerResource[F[_]] private[kafka] (
  private val F: ConcurrentEffect[F]
) extends AnyVal {

  /**
    * Creates a new [[KafkaConsumer]] in the `Resource` context.
    * This is equivalent to using `consumerResource` directly,
    * except we're able to infer the key and value type.
    */
  def using[K, V](settings: ConsumerSettings[K, V])(
    implicit context: ContextShift[F],
    timer: Timer[F]
  ): Resource[F, KafkaConsumer[F, K, V]] =
    consumerResource(settings)(F, context, timer)

  override def toString: String =
    "ConsumerResource$" + System.identityHashCode(this)
}

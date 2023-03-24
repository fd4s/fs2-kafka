/*
 * Copyright 2018-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.{ConcurrentEffect, ContextShift, Timer}
import fs2.Stream

/**
  * [[ConsumerStream]] provides support for inferring the key and value
  * type from [[ConsumerSettings]] when using `KafkaConsumer.stream` with the
  * following syntax.
  *
  * {{{
  * KafkaConsumer.stream[F].using(settings)
  * }}}
  */
final class ConsumerStream[F[_]] private[kafka] (
  private val F: ConcurrentEffect[F]
) extends AnyVal {

  /**
    * Creates a new [[KafkaConsumer]] in the `Stream` context.
    * This is equivalent to using `KafkaConsumer.stream` directly,
    * except we're able to infer the key and value type.
    */
  def using[K, V](settings: ConsumerSettings[F, K, V])(
    implicit context: ContextShift[F],
    timer: Timer[F]
  ): Stream[F, KafkaConsumer[F, K, V]] =
    KafkaConsumer.stream(settings)(F, context, timer)

  override def toString: String =
    "ConsumerStream$" + System.identityHashCode(this)
}

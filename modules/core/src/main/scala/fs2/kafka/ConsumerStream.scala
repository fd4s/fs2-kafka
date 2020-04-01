/*
 * Copyright 2018-2020 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
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
  def using[K, V](settings: ConsumerSettings[F, K, V])(
    implicit context: ContextShift[F],
    timer: Timer[F]
  ): Stream[F, KafkaConsumer.Metrics[F, K, V]] =
    consumerStream(settings)(F, context, timer)

  override def toString: String =
    "ConsumerStream$" + System.identityHashCode(this)
}

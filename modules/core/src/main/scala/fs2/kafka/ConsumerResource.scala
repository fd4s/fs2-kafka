/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.{ConcurrentEffect, ContextShift, Resource, Timer}

/**
  * [[ConsumerResource]] provides support for inferring the key and value
  * type from [[ConsumerSettings]] when using `consumerResource` with the
  * following syntax.
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
  def using[K, V](settings: ConsumerSettings[F, K, V])(
    implicit context: ContextShift[F],
    timer: Timer[F]
  ): Resource[F, KafkaConsumer[F, K, V]] =
    consumerResource(settings)(F, context, timer)

  override def toString: String =
    "ConsumerResource$" + System.identityHashCode(this)
}

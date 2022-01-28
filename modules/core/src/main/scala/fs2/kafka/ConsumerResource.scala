/*
 * Copyright 2018-2022 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.{ConcurrentEffect, Resource}
import cats.effect.Temporal

/**
  * [[ConsumerResource]] provides support for inferring the key and value
  * type from [[ConsumerSettings]] when using `KafkaConsumer.resource` with the
  * following syntax.
  *
  * {{{
  * KafkaConsumer.resource[F].using(settings)
  * }}}
  */
final class ConsumerResource[F[_]] private[kafka] (
  private val F: ConcurrentEffect[F]
) extends AnyVal {

  /**
    * Creates a new [[KafkaConsumer]] in the `Resource` context.
    * This is equivalent to using `KafkaConsumer.resource` directly,
    * except we're able to infer the key and value type.
    */
  def using[K, V](settings: ConsumerSettings[F, K, V])(
    implicit
    timer: Temporal[F]
  ): Resource[F, KafkaConsumer[F, K, V]] =
    KafkaConsumer.resource(settings)(F, context, timer)

  override def toString: String =
    "ConsumerResource$" + System.identityHashCode(this)
}

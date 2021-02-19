/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.{Resource, Async}

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
  private val F: Async[F]
) extends AnyVal {

  /**
    * Creates a new [[KafkaConsumer]] in the `Resource` context.
    * This is equivalent to using `KafkaConsumer.resource` directly,
    * except we're able to infer the key and value type.
    */
  def using[K, V](
    settings: ConsumerSettings[F, K, V]
  ): Resource[F, KafkaConsumer[F, K, V]] =
    KafkaConsumer.resource[F, K, V](settings)(F)

  override def toString: String =
    "ConsumerResource$" + System.identityHashCode(this)
}

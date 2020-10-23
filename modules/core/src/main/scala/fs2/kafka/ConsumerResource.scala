/*
 * Copyright 2018-2020 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.{Concurrent, Resource, Async}

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
  private val F: Async[F]
) extends AnyVal {

  /**
    * Creates a new [[KafkaConsumer]] in the `Resource` context.
    * This is equivalent to using `consumerResource` directly,
    * except we're able to infer the key and value type.
    */
  def using[K, V](
    settings: ConsumerSettings[F, K, V]
  ): Resource[F, KafkaConsumer[F, K, V]] =
    consumerResource(settings)(F)

  override def toString: String =
    "ConsumerResource$" + System.identityHashCode(this)
}

/*
 * Copyright 2018-2020 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.{Concurrent, ContextShift, Resource}

/**
  * [[ProducerResource]] provides support for inferring the key and value
  * type from [[ProducerSettings]] when using `producerResource` with the
  * following syntax.
  *
  * {{{
  * producerResource[F].using(settings)
  * }}}
  */
final class ProducerResource[F[_]] private[kafka] (
  private val F: Concurrent[F]
) extends AnyVal {

  /**
    * Creates a new [[KafkaProducer]] in the `Resource` context.
    * This is equivalent to using `producerResource` directly,
    * except we're able to infer the key and value type.
    */
  def using[K, V](settings: ProducerSettings[F, K, V])(
    implicit context: ContextShift[F]
  ): Resource[F, KafkaProducer.Metrics[F, K, V]] =
    producerResource(settings)(F, context)

  override def toString: String =
    "ProducerResource$" + System.identityHashCode(this)
}

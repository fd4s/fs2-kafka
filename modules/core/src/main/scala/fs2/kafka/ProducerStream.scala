/*
 * Copyright 2018-2020 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.{ConcurrentEffect, ContextShift}
import fs2.Stream

/**
  * [[ProducerStream]] provides support for inferring the key and value
  * type from [[ProducerSettings]] when using `producerStream` with the
  * following syntax.
  *
  * {{{
  * producerStream[F].using(settings)
  * }}}
  */
final class ProducerStream[F[_]] private[kafka] (
  private val F: ConcurrentEffect[F]
) extends AnyVal {

  /**
    * Creates a new [[KafkaProducer]] in the `Stream` context.
    * This is equivalent to using `producerStream` directly,
    * except we're able to infer the key and value type.
    */
  def using[K, V](settings: ProducerSettings[F, K, V])(
    implicit context: ContextShift[F]
  ): Stream[F, KafkaProducer.Metrics[F, K, V]] =
    producerStream(settings)(F, context)

  override def toString: String =
    "ProducerStream$" + System.identityHashCode(this)
}

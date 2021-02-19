/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.Async
import fs2.Stream

/**
  * [[ProducerStream]] provides support for inferring the key and value
  * type from [[ProducerSettings]] when using `KafkaProducer.stream` with the
  * following syntax.
  *
  * {{{
  * KafkaProducer.stream[F].using(settings)
  * }}}
  */
final class ProducerStream[F[_]] private[kafka] (
  private val F: Async[F]
) extends AnyVal {

  /**
    * Creates a new [[KafkaProducer]] in the `Stream` context.
    * This is equivalent to using `KafkaProducer.stream` directly,
    * except we're able to infer the key and value type.
    */
  def using[K, V](settings: ProducerSettings[F, K, V]): Stream[F, KafkaProducer.Metrics[F, K, V]] =
    KafkaProducer.stream(settings)(F)

  override def toString: String =
    "ProducerStream$" + System.identityHashCode(this)
}

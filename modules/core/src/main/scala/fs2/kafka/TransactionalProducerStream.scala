/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.ConcurrentEffect
import fs2.Stream

/**
  * [[TransactionalProducerStream]] provides support for inferring
  * the key and value type from [[TransactionalProducerSettings]]
  * when using `TransactionalKafkaProducer.stream` with the following syntax.
  *
  * {{{
  * TransactionalKafkaProducer.stream[F].using(settings)
  * }}}
  */
final class TransactionalProducerStream[F[_]] private[kafka] (
  private val F: ConcurrentEffect[F]
) extends AnyVal {

  /**
    * Creates a new [[TransactionalKafkaProducer]] in the `Stream` context.
    * This is equivalent to using `TransactionalKafkaProducer.stream` directly,
    * except we're able to infer the key and value type.
    */
  def using[K, V](settings: TransactionalProducerSettings[F, K, V]): Stream[F, TransactionalKafkaProducer.Metrics[F, K, V]] =
    TransactionalKafkaProducer.stream(settings)(F, context)

  override def toString: String =
    "TransactionalProducerStream$" + System.identityHashCode(this)
}

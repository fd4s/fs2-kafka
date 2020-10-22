/*
 * Copyright 2018-2020 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.Async
import fs2.Stream

/**
  * [[TransactionalProducerStream]] provides support for inferring
  * the key and value type from [[TransactionalProducerSettings]]
  * when using `transactionalProducerStream` with the following syntax.
  *
  * {{{
  * transactionalProducerStream[F].using(settings)
  * }}}
  */
final class TransactionalProducerStream[F[_]] private[kafka] (
  private val F: Async[F]
) extends AnyVal {

  /**
    * Creates a new [[TransactionalKafkaProducer]] in the `Stream` context.
    * This is equivalent to using `transactionalProducerStream` directly,
    * except we're able to infer the key and value type.
    */
  def using[K, V](
    settings: TransactionalProducerSettings[F, K, V]
  ): Stream[F, TransactionalKafkaProducer[F, K, V]] =
    transactionalProducerStream(settings)(F)

  override def toString: String =
    "TransactionalProducerStream$" + System.identityHashCode(this)
}

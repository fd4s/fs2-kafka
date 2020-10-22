/*
 * Copyright 2018-2020 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.{Async, Resource}

/**
  * [[TransactionalProducerResource]] provides support for inferring
  * the key and value type from [[TransactionalProducerSettings]]
  * when using `transactionalProducerResource` with the following syntax.
  *
  * {{{
  * transactionalProducerResource[F].using(settings)
  * }}}
  */
final class TransactionalProducerResource[F[_]] private[kafka] (
  private val F: Async[F]
) extends AnyVal {

  /**
    * Creates a new [[TransactionalKafkaProducer]] in the `Resource` context.
    * This is equivalent to using `transactionalProducerResource` directly,
    * except we're able to infer the key and value type.
    */
  def using[K, V](
    settings: TransactionalProducerSettings[F, K, V]
  ): Resource[F, TransactionalKafkaProducer[F, K, V]] =
    transactionalProducerResource(settings)(F)

  override def toString: String =
    "TransactionalProducerResource$" + System.identityHashCode(this)
}

/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.{ConcurrentEffect, Resource}

/**
  * [[TransactionalProducerResource]] provides support for inferring
  * the key and value type from [[TransactionalProducerSettings]]
  * when using `TransactionalKafkaProducer.resource` with the following syntax.
  *
  * {{{
  * TransactionalKafkaProducer.resource[F].using(settings)
  * }}}
  */
final class TransactionalProducerResource[F[_]] private[kafka] (
  private val F: ConcurrentEffect[F]
) extends AnyVal {

  /**
    * Creates a new [[TransactionalKafkaProducer]] in the `Resource` context.
    * This is equivalent to using `TransactionalKafkaProducer.resource` directly,
    * except we're able to infer the key and value type.
    */
  def using[K, V](settings: TransactionalProducerSettings[F, K, V]): Resource[F, TransactionalKafkaProducer.Metrics[F, K, V]] =
    TransactionalKafkaProducer.resource(settings)(F, context)

  override def toString: String =
    "TransactionalProducerResource$" + System.identityHashCode(this)
}

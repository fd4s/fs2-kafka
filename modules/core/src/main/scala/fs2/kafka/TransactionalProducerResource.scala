/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.{Concurrent, ContextShift, Resource}

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
  private val F: Concurrent[F]
) extends AnyVal {

  /**
    * Creates a new [[TransactionalKafkaProducer]] in the `Resource` context.
    * This is equivalent to using `transactionalProducerResource` directly,
    * except we're able to infer the key and value type.
    */
  def using[K, V](settings: TransactionalProducerSettings[F, K, V])(
    implicit context: ContextShift[F]
  ): Resource[F, TransactionalKafkaProducer[F, K, V]] =
    transactionalProducerResource(settings)(F, context)

  override def toString: String =
    "TransactionalProducerResource$" + System.identityHashCode(this)
}

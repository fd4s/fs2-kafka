/*
 * Copyright 2018-2020 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.{Concurrent, Temporal, Async}
import fs2.Stream

/**
  * [[ConsumerStream]] provides support for inferring the key and value
  * type from [[ConsumerSettings]] when using `consumerStream` with the
  * following syntax.
  *
  * {{{
  * consumerStream[F].using(settings)
  * }}}
  */
final class ConsumerStream[F[_]] private[kafka] (
  private val F: Async[F]
) extends AnyVal {

  /**
    * Creates a new [[KafkaConsumer]] in the `Stream` context.
    * This is equivalent to using `consumerStream` directly,
    * except we're able to infer the key and value type.
    */
  def using[K, V](settings: ConsumerSettings[F, K, V]): Stream[F, KafkaConsumer[F, K, V]] =
    consumerStream(settings)(F, ???)

  override def toString: String =
    "ConsumerStream$" + System.identityHashCode(this)
}

/*
 * Copyright 2018-2022 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import fs2.Chunk

object TransactionalProducerRecords {

  @deprecated("this is now an identity operation", "3.0.0-M5")
  def apply[F[_], K, V](
    chunk: Chunk[CommittableProducerRecords[F, K, V]]
  ): Chunk[CommittableProducerRecords[F, K, V]] = chunk

  /**
    * Creates a new [[TransactionalProducerRecords]] for producing exactly
    * one [[CommittableProducerRecords]], emitting a [[ProducerResult]]
    * with the result and specified passthrough value.
    */
  def one[F[_], K, V](
    record: CommittableProducerRecords[F, K, V]
  ): TransactionalProducerRecords[F, K, V] =
    Chunk.singleton(record)

}

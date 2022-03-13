/*
 * Copyright 2018-2022 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.Show
import cats.syntax.show._
import fs2.Chunk
import fs2.kafka.internal.syntax._

/**
  * Represents zero or more [[CommittableProducerRecords]], together with
  * arbitrary passthrough value, all of which can be used together with a
  * [[TransactionalKafkaProducer]] to produce records and commit offsets
  * within a single transaction.<br>
  * <br>
  * [[TransactionalProducerRecords]]s can be created using one of the
  * following options.<br>
  * <br>
  * - `TransactionalProducerRecords#apply` to produce zero or more records,
  * commit the offsets, and then emit a [[ProducerResult]] with the results
  * and specified passthrough value.<br>
  * - `TransactionalProducerRecords#one` to produce zero or more records,
  * commit exactly one offset, then emit a [[ProducerResult]] with the
  * results and specified passthrough value.
  */
sealed abstract class TransactionalProducerRecords[F[_], +K, +V] {

  /** The records to produce and commit. Can be empty for passthrough-only. */
  def records: Chunk[CommittableProducerRecords[F, K, V]]
}

object TransactionalProducerRecords {
  private[this] final class TransactionalProducerRecordsImpl[F[_], +K, +V](
    override val records: Chunk[CommittableProducerRecords[F, K, V]]
  ) extends TransactionalProducerRecords[F, K, V] {
    override def toString: String =
      if (records.isEmpty) s"TransactionalProducerRecords(<empty>)"
      else records.mkString("TransactionalProducerRecords(", ", ", ")")
  }

  /**
    * Creates a new [[TransactionalProducerRecords]] for producing zero or
    * more [[CommittableProducerRecords]], emitting a [[ProducerResult]]
    * with the results and `Unit` passthrough value.
    */
  def apply[F[_], K, V](
    records: Chunk[CommittableProducerRecords[F, K, V]]
  ): TransactionalProducerRecords[F, K, V] =
    new TransactionalProducerRecordsImpl(records)

  /**
    * Creates a new [[TransactionalProducerRecords]] for producing exactly
    * one [[CommittableProducerRecords]], emitting a [[ProducerResult]]
    * with the result and specified passthrough value.
    */
  def one[F[_], K, V](
    record: CommittableProducerRecords[F, K, V]
  ): TransactionalProducerRecords[F, K, V] =
    apply(Chunk.singleton(record))

  implicit def transactionalProducerRecordsShow[F[_], P, K, V](
    implicit
    K: Show[K],
    V: Show[V]
  ): Show[TransactionalProducerRecords[F, K, V]] =
    Show.show { records =>
      if (records.records.isEmpty)
        show"TransactionalProducerRecords(<empty>)"
      else
        records.records.mkStringShow(
          "TransactionalProducerRecords(",
          ", ",
          ")"
        )
    }
}

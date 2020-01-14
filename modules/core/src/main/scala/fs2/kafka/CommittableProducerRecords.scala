/*
 * Copyright 2018-2020 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.{Foldable, Show}
import cats.syntax.show._
import fs2.Chunk
import fs2.kafka.internal.syntax._

import scala.collection.mutable

/**
  * [[CommittableProducerRecords]] represents zero or more [[ProducerRecord]]s
  * and a [[CommittableOffset]], used by [[TransactionalKafkaProducer]] to
  * produce the records and commit the offset atomically.<br>
  * <br>
  * [[CommittableProducerRecords]]s can be created using one of the following options.<br>
  * <br>
  * - `CommittableProducerRecords#apply` to produce zero or more records
  * within the same transaction as the offset is committed.<br>
  * - `CommittableProducerRecords#one` to produce exactly one record within
  * the same transaction as the offset is committed.
  */
sealed abstract class CommittableProducerRecords[F[_], +K, +V] {

  /** The records to produce. Can be empty to simply commit the offset. */
  def records: Chunk[ProducerRecord[K, V]]

  /** The offset to commit. */
  def offset: CommittableOffset[F]
}

object CommittableProducerRecords {
  private[this] final class CommittableProducerRecordsImpl[F[_], +K, +V](
    override val records: Chunk[ProducerRecord[K, V]],
    override val offset: CommittableOffset[F]
  ) extends CommittableProducerRecords[F, K, V] {
    override def toString: String = {
      if (records.isEmpty) s"CommittableProducerRecords(<empty>, $offset)"
      else records.mkString("CommittableProducerRecords(", ", ", s", $offset)")
    }
  }

  /**
    * Creates a new [[CommittableProducerRecords]] for producing zero or
    * more [[ProducerRecord]]s and committing an offset atomically within
    * a transaction.
    */
  def apply[F[_], G[+_], K, V](
    records: G[ProducerRecord[K, V]],
    offset: CommittableOffset[F]
  )(implicit G: Foldable[G]): CommittableProducerRecords[F, K, V] = {
    val numRecords = G.size(records).toInt
    val chunk = if (numRecords <= 1) {
      G.get(records)(0) match {
        case None         => Chunk.empty[ProducerRecord[K, V]]
        case Some(record) => Chunk.singleton(record)
      }
    } else {
      val buf = new mutable.ArrayBuffer[ProducerRecord[K, V]](numRecords)
      G.foldLeft(records, ()) {
        case (_, record) =>
          buf += record
          ()
      }
      Chunk.buffer(buf)
    }

    new CommittableProducerRecordsImpl(chunk, offset)
  }

  /**
    * Creates a new [[CommittableProducerRecords]] for producing exactly
    * one [[ProducerRecord]] and committing an offset atomically within
    * a transaction.
    */
  def one[F[_], K, V](
    record: ProducerRecord[K, V],
    offset: CommittableOffset[F]
  ): CommittableProducerRecords[F, K, V] =
    new CommittableProducerRecordsImpl(Chunk.singleton(record), offset)

  implicit def committableProducerRecordsShow[F[_], K, V](
    implicit
    K: Show[K],
    V: Show[V]
  ): Show[CommittableProducerRecords[F, K, V]] =
    Show.show { committable =>
      if (committable.records.isEmpty)
        show"CommittableProducerRecords(<empty>, ${committable.offset})"
      else
        committable.records.mkStringShow(
          "CommittableProducerRecords(",
          ", ",
          s", ${committable.offset})"
        )
    }
}

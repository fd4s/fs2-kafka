/*
 * Copyright 2018-2022 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.{Show, Traverse}
import cats.syntax.show._
import fs2.Chunk
import fs2.kafka.internal.syntax._

import scala.collection.mutable

/**
  * [[ProducerRecords]] represents zero or more `ProducerRecord`s,
  * together with an arbitrary passthrough value, all of which can
  * be used with [[KafkaProducer]]. [[ProducerRecords]]s can be
  * created using one of the following options.<br>
  * <br>
  * - `ProducerRecords#apply` to produce zero or more records
  * and then emit a [[ProducerResult]] with the results and
  * specified passthrough value.<br>
  * - `ProducerRecords#one` to produce exactly one record and
  * then emit a [[ProducerResult]] with the result and specified
  * passthrough value.<br>
  * <br>
  * The [[passthrough]] and [[records]] can be retrieved from an
  * existing [[ProducerRecords]] instance.<br>
  */
sealed abstract class ProducerRecords[+K, +V] {

  /** The records to produce. Can be empty for passthrough-only. */
  def records: Chunk[ProducerRecord[K, V]]
}

object ProducerRecords {
  private[this] final class ProducerRecordsImpl[+K, +V](
    override val records: Chunk[ProducerRecord[K, V]]
  ) extends ProducerRecords[K, V] {
    override def toString: String =
      if (records.isEmpty) s"ProducerRecords(<empty>)"
      else records.mkString("ProducerRecords(", ", ", ")")
  }

  /**
    * Creates a new [[ProducerRecords]] for producing zero or more
    * `ProducerRecords`s, then emitting a [[ProducerResult]] with
    * the results and specified passthrough value.
    */
  def apply[F[+_], K, V](
    records: F[ProducerRecord[K, V]]
  )(
    implicit F: Traverse[F]
  ): ProducerRecords[K, V] = {
    val numRecords = F.size(records).toInt
    val chunk = if (numRecords <= 1) {
      F.get(records)(0) match {
        case None         => Chunk.empty[ProducerRecord[K, V]]
        case Some(record) => Chunk.singleton(record)
      }
    } else {
      val buf = new mutable.ArrayBuffer[ProducerRecord[K, V]](numRecords)
      F.foldLeft(records, ()) {
        case (_, record) =>
          buf += record
          ()
      }
      Chunk.array(buf.toArray)
    }
    new ProducerRecordsImpl(chunk)
  }

  /**
    * Creates a new [[ProducerRecords]] for producing exactly one
    * `ProducerRecord`, then emitting a [[ProducerResult]] with
    * the result and specified passthrough value.
    */
  def one[K, V](
    record: ProducerRecord[K, V]
  ): ProducerRecords[K, V] =
    new ProducerRecordsImpl(Chunk.singleton(record))

  implicit def producerRecordsShow[K, V](
    implicit
    K: Show[K],
    V: Show[V]
  ): Show[ProducerRecords[K, V]] = Show.show { records =>
    if (records.records.isEmpty) show"ProducerRecords(<empty>})"
    else records.records.mkStringShow("ProducerRecords(", ", ", ")")
  }
}

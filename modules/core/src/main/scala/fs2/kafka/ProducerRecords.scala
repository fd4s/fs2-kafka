/*
 * Copyright 2018-2022 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import fs2.Chunk

import scala.collection.mutable
import cats.Traverse

object ProducerRecords {

  def apply[F[+_], K, V](
    records: F[ProducerRecord[K, V]]
  )(
    implicit F: Traverse[F]
  ): ProducerRecords[K, V] = {
    val numRecords = F.size(records).toInt
    if (numRecords <= 1) {
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
  }

  def one[K, V](record: ProducerRecord[K, V]): ProducerRecords[K, V] =
    Chunk.singleton(record)

}

/*
 * Copyright 2018-2019 OVO Energy Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
    val numRecords = G.size(records)
    val chunk = if (numRecords <= 1) {
      G.get(records)(0) match {
        case None         => Chunk.empty[ProducerRecord[K, V]]
        case Some(record) => Chunk.singleton(record)
      }
    } else {
      val buf = new mutable.ArrayBuffer[ProducerRecord[K, V]](G.size(records).toInt)
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

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
sealed abstract class ProducerRecords[+K, +V, +P] {

  /** The records to produce. Can be empty for passthrough-only. */
  def records: Chunk[ProducerRecord[K, V]]

  /** The passthrough to emit once all [[records]] have been produced. */
  def passthrough: P
}

object ProducerRecords {
  private[this] final class ProducerRecordsImpl[+K, +V, +P](
    override val records: Chunk[ProducerRecord[K, V]],
    override val passthrough: P
  ) extends ProducerRecords[K, V, P] {
    override def toString: String = {
      if (records.isEmpty) s"ProducerRecords(<empty>, $passthrough)"
      else records.mkString("ProducerRecords(", ", ", s", $passthrough)")
    }
  }

  /**
    * Creates a new [[ProducerRecords]] for producing zero or more
    * `ProducerRecords`s, then emitting a [[ProducerResult]] with
    * the results and `Unit` passthrough value.
    */
  def apply[F[+_], K, V](
    records: F[ProducerRecord[K, V]]
  )(
    implicit F: Traverse[F]
  ): ProducerRecords[K, V, Unit] =
    apply(records, ())

  /**
    * Creates a new [[ProducerRecords]] for producing zero or more
    * `ProducerRecords`s, then emitting a [[ProducerResult]] with
    * the results and specified passthrough value.
    */
  def apply[F[+_], K, V, P](
    records: F[ProducerRecord[K, V]],
    passthrough: P
  )(
    implicit F: Traverse[F]
  ): ProducerRecords[K, V, P] = {
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
      Chunk.buffer(buf)
    }
    new ProducerRecordsImpl(chunk, passthrough)
  }

  def unapply[K, V, P](prs: ProducerRecords[K, V, P]): Some[(Chunk[ProducerRecord[K, V]], P)] =
    Some((prs.records, prs.passthrough))

  /**
    * Creates a new [[ProducerRecords]] for producing exactly one
    * `ProducerRecord`, then emitting a [[ProducerResult]] with
    * the result and `Unit` passthrough value.
    */
  def one[K, V](
    record: ProducerRecord[K, V]
  ): ProducerRecords[K, V, Unit] =
    one(record, ())

  /**
    * Creates a new [[ProducerRecords]] for producing exactly one
    * `ProducerRecord`, then emitting a [[ProducerResult]] with
    * the result and specified passthrough value.
    */
  def one[K, V, P](
    record: ProducerRecord[K, V],
    passthrough: P
  ): ProducerRecords[K, V, P] =
    new ProducerRecordsImpl(Chunk.singleton(record), passthrough)

  implicit def producerRecordsShow[K, V, P](
    implicit
    K: Show[K],
    V: Show[V],
    P: Show[P]
  ): Show[ProducerRecords[K, V, P]] = Show.show { records =>
    if (records.records.isEmpty) show"ProducerRecords(<empty>, ${records.passthrough})"
    else records.records.mkStringShow("ProducerRecords(", ", ", s", ${records.passthrough})")
  }
}

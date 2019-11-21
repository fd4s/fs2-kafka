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
sealed abstract class TransactionalProducerRecords[F[_], +K, +V, +P] {

  /** The records to produce and commit. Can be empty for passthrough-only. */
  def records: Chunk[CommittableProducerRecords[F, K, V]]

  /** The passthrough to emit once all [[records]] have been produced and committed. */
  def passthrough: P
}

object TransactionalProducerRecords {
  private[this] final class TransactionalProducerRecordsImpl[F[_], +K, +V, +P](
    override val records: Chunk[CommittableProducerRecords[F, K, V]],
    override val passthrough: P
  ) extends TransactionalProducerRecords[F, K, V, P] {
    override def toString: String =
      if (records.isEmpty) s"TransactionalProducerRecords(<empty>, $passthrough)"
      else records.mkString("TransactionalProducerRecords(", ", ", s", $passthrough)")
  }

  /**
    * Creates a new [[TransactionalProducerRecords]] for producing zero or
    * more [[CommittableProducerRecords]], emitting a [[ProducerResult]]
    * with the results and `Unit` passthrough value.
    */
  def apply[F[_], K, V](
    records: Chunk[CommittableProducerRecords[F, K, V]]
  ): TransactionalProducerRecords[F, K, V, Unit] =
    apply(records, ())

  /**
    * Creates a new [[TransactionalProducerRecords]] for producing zero or
    * more [[CommittableProducerRecords]], emitting a [[ProducerResult]]
    * with the results and specified passthrough value.
    */
  def apply[F[_], K, V, P](
    records: Chunk[CommittableProducerRecords[F, K, V]],
    passthrough: P
  ): TransactionalProducerRecords[F, K, V, P] =
    new TransactionalProducerRecordsImpl(records, passthrough)

  def unapply[F[_], K, V, P](
    tpr: TransactionalProducerRecords[F, K, V, P]
  ): Some[(Chunk[CommittableProducerRecords[F, K, V]], P)] =
    Some((tpr.records, tpr.passthrough))

  /**
    * Creates a new [[TransactionalProducerRecords]] for producing exactly
    * one [[CommittableProducerRecords]], emitting a [[ProducerResult]]
    * with the result and `Unit` passthrough value.
    */
  def one[F[_], K, V](
    record: CommittableProducerRecords[F, K, V]
  ): TransactionalProducerRecords[F, K, V, Unit] =
    one(record, ())

  /**
    * Creates a new [[TransactionalProducerRecords]] for producing exactly
    * one [[CommittableProducerRecords]], emitting a [[ProducerResult]]
    * with the result and specified passthrough value.
    */
  def one[F[_], K, V, P](
    record: CommittableProducerRecords[F, K, V],
    passthrough: P
  ): TransactionalProducerRecords[F, K, V, P] =
    apply(Chunk.singleton(record), passthrough)

  implicit def transactionalProducerRecordsShow[F[_], K, V, P](
    implicit
    K: Show[K],
    V: Show[V],
    P: Show[P]
  ): Show[TransactionalProducerRecords[F, K, V, P]] =
    Show.show { records =>
      if (records.records.isEmpty)
        show"TransactionalProducerRecords(<empty>, ${records.passthrough})"
      else
        records.records.mkStringShow(
          "TransactionalProducerRecords(",
          ", ",
          show", ${records.passthrough})"
        )
    }
}

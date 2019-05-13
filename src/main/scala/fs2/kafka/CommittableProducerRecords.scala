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

import cats.data.Chain

/**
  * [[CommittableProducerRecords]] represents zero or more [[ProducerRecord]]s
  * and a [[CommittableOffset]] which can be used by [[TransactionalKafkaProducer]]
  * to produce the records and commit the offset atomically.
  * [[CommittableProducerRecords]]s can be created using one of the following options.<br>
  * <br>
  * - `CommittableProducerRecords#apply` to produce zero or more records
  * within the same transaction that an offset is committed.<br>
  * - `CommittableProducerRecords#one` to produce exactly one record within
  * the same transaction that an offset is committed.<br>
  * <br>
  */
sealed abstract class CommittableProducerRecords[F[_], G[+ _], +K, +V] {

  /** The records to produce. Can be empty to simply commit the offset. */
  def records: G[ProducerRecord[K, V]]

  /** The offset to commit. */
  def committableOffset: CommittableOffset[F]
}

object CommittableProducerRecords {
  private[this] final class CommittableProducerRecordsImpl[F[_], G[+ _], +K, +V](
    override val records: G[ProducerRecord[K, V]],
    override val committableOffset: CommittableOffset[F]
  ) extends CommittableProducerRecords[F, G, K, V] {
    override def toString: String =
      s"CommittableProducerRecords($records, $committableOffset)"
  }

  /**
    * Creates a new [[CommittableProducerRecords]] for producing zero or
    * more [[ProducerRecord]]s and committing an offset atomically within
    * a transaction.
    */
  def apply[F[_], G[+ _], K, V](
    records: G[ProducerRecord[K, V]],
    committableOffset: CommittableOffset[F]
  ): CommittableProducerRecords[F, G, K, V] =
    new CommittableProducerRecordsImpl(records, committableOffset)

  /**
    * Creates a new [[CommittableProducerRecords]] for producing exactly
    * one [[ProducerRecord]] and committing an offset atomically within
    * a transaction.
    */
  def one[F[_], K, V](
    record: ProducerRecord[K, V],
    committableOffset: CommittableOffset[F]
  ): CommittableProducerRecords[F, Chain, K, V] =
    apply(Chain.one(record), committableOffset)
}

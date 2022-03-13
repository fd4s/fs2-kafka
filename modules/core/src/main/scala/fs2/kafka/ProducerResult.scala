/*
 * Copyright 2018-2022 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.syntax.show._
import cats.Show
import fs2.Chunk
import fs2.kafka.instances._
import fs2.kafka.internal.syntax._
import org.apache.kafka.clients.producer.RecordMetadata

/**
  * [[ProducerResult]] represents the result of having produced zero
  * or more `ProducerRecord`s from a [[ProducerRecords]]. Finally, a
  * passthrough value and `ProducerRecord`s along with respective
  * `RecordMetadata` are emitted in a [[ProducerResult]].<br>
  * <br>
  * The [[passthrough]] and [[records]] can be retrieved from an
  * existing [[ProducerResult]] instance.<br>
  * <br>
  * Use [[ProducerResult#apply]] to create a new [[ProducerResult]].
  */
sealed abstract class ProducerResult[+K, +V] {

  /**
    * The records produced along with respective metadata.
    * Can be empty for passthrough-only.
    */
  def records: Chunk[(ProducerRecord[K, V], RecordMetadata)]
}

object ProducerResult {
  private[this] final class ProducerResultImpl[+K, +V](
    override val records: Chunk[(ProducerRecord[K, V], RecordMetadata)]
  ) extends ProducerResult[K, V] {

    override def toString: String =
      if (records.isEmpty)
        s"ProducerResult(<empty>)"
      else
        records.mkStringAppend {
          case (append, (record, metadata)) =>
            append(metadata.toString)
            append(" -> ")
            append(record.toString)
        }(
          start = "ProducerResult(",
          sep = ", ",
          end = s")"
        )
  }

  /**
    * Creates a new [[ProducerResult]] for having produced zero
    * or more `ProducerRecord`s, finally emitting a passthrough
    * value and the `ProducerRecord`s with `RecordMetadata`.
    */
  def apply[K, V](
    records: Chunk[(ProducerRecord[K, V], RecordMetadata)]
  ): ProducerResult[K, V] =
    new ProducerResultImpl(records)

  implicit def producerResultShow[K, V](
    implicit
    K: Show[K],
    V: Show[V]
  ): Show[ProducerResult[K, V]] = Show.show { result =>
    if (result.records.isEmpty)
      show"ProducerResult(<empty>)"
    else
      result.records.mkStringAppend {
        case (append, (record, metadata)) =>
          append(metadata.show)
          append(" -> ")
          append(record.show)
      }(
        start = "ProducerResult(",
        sep = ", ",
        end = ")"
      )
  }
}

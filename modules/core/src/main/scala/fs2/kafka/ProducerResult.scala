/*
 * Copyright 2018-2023 OVO Energy Limited
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
sealed abstract class ProducerResult[+P, +K, +V] {

  /**
    * The records produced along with respective metadata.
    * Can be empty for passthrough-only.
    */
  def records: Chunk[(ProducerRecord[K, V], RecordMetadata)]

  /** The passthrough value. */
  def passthrough: P
}

object ProducerResult {
  private[this] final class ProducerResultImpl[+P, +K, +V](
    override val records: Chunk[(ProducerRecord[K, V], RecordMetadata)],
    override val passthrough: P
  ) extends ProducerResult[P, K, V] {

    override def toString: String =
      if (records.isEmpty)
        s"ProducerResult(<empty>, $passthrough)"
      else
        records.mkStringAppend {
          case (append, (record, metadata)) =>
            append(metadata.toString)
            append(" -> ")
            append(record.toString)
        }(
          start = "ProducerResult(",
          sep = ", ",
          end = s", $passthrough)"
        )
  }

  /**
    * Creates a new [[ProducerResult]] for having produced zero
    * or more `ProducerRecord`s, finally emitting a passthrough
    * value and the `ProducerRecord`s with `RecordMetadata`.
    */
  def apply[P, K, V](
    records: Chunk[(ProducerRecord[K, V], RecordMetadata)],
    passthrough: P
  ): ProducerResult[P, K, V] =
    new ProducerResultImpl(records, passthrough)

  implicit def producerResultShow[P, K, V](
    implicit
    K: Show[K],
    V: Show[V],
    P: Show[P]
  ): Show[ProducerResult[P, K, V]] = Show.show { result =>
    if (result.records.isEmpty)
      show"ProducerResult(<empty>, ${result.passthrough})"
    else
      result.records.mkStringAppend {
        case (append, (record, metadata)) =>
          append(metadata.show)
          append(" -> ")
          append(record.show)
      }(
        start = "ProducerResult(",
        sep = ", ",
        end = show", ${result.passthrough})"
      )
  }
}

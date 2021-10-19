/*
 * Copyright 2018-2021 OVO Energy Limited
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
sealed abstract class ProducerResult[+K, +V, +P] {
  /**
    * The records produced along with respective metadata.
    * Can be empty for passthrough-only.
    */
  def records: Chunk[(ProducerRecord[K, V], RecordMetadata)]

  /** The passthrough value. */
  def passthrough: P
}

object ProducerResult {
  private[this] final class ProducerResultImpl[+K, +V, +P](
    override val records: Chunk[(ProducerRecord[K, V], RecordMetadata)],
    override val passthrough: P
  ) extends ProducerResult[K, V, P] {
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
  def apply[K, V, P](
    records: Chunk[(ProducerRecord[K, V], RecordMetadata)],
    passthrough: P
  ): ProducerResult[K, V, P] =
    new ProducerResultImpl(records, passthrough)

  implicit def producerResultShow[K, V, P](
    implicit
    K: Show[K],
    V: Show[V],
    P: Show[P]
  ): Show[ProducerResult[K, V, P]] = Show.show { result =>
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

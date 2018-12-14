/*
 * Copyright 2018 OVO Energy Limited
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

import cats.syntax.foldable._
import cats.syntax.show._
import cats.{Foldable, Show}
import fs2.kafka.internal.instances._
import fs2.kafka.internal.syntax._
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}

/**
  * [[ProducerResult]] represents the result of having produced zero
  * or more `ProducerRecord`s from a [[ProducerMessage]]. Finally, a
  * passthrough value and `ProducerRecord`s along with respective
  * `RecordMetadata` are emitted in a [[ProducerResult]].<br>
  * <br>
  * The [[passthrough]] and [[records]] can be retrieved from an
  * existing [[ProducerResult]] instance.<br>
  * <br>
  * Use [[ProducerResult#apply]] to create a new [[ProducerResult]].
  */
sealed abstract class ProducerResult[F[_], K, V, +P] {

  /**
    * The records produced along with respective metadata.
    * Can be empty for passthrough-only.
    */
  def records: F[(ProducerRecord[K, V], RecordMetadata)]

  /** The passthrough value. */
  def passthrough: P
}

object ProducerResult {
  private[this] final class ProducerResultImpl[F[_], K, V, +P](
    override val records: F[(ProducerRecord[K, V], RecordMetadata)],
    override val passthrough: P
  )(implicit F: Foldable[F])
      extends ProducerResult[F, K, V, P] {

    override def toString: String = {
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
  }

  /**
    * Creates a new [[ProducerResult]] for having produced zero
    * or more `ProducerRecord`s, finally emitting a passthrough
    * value and the `ProducerRecord`s with `RecordMetadata`.
    */
  def apply[F[_], K, V, P](
    records: F[(ProducerRecord[K, V], RecordMetadata)],
    passthrough: P
  )(
    implicit F: Foldable[F]
  ): ProducerResult[F, K, V, P] =
    new ProducerResultImpl(records, passthrough)

  implicit def producerResultShow[F[_], K, V, P](
    implicit
    F: Foldable[F],
    K: Show[K],
    V: Show[V],
    P: Show[P]
  ): Show[ProducerResult[F, K, V, P]] = Show.show { result =>
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

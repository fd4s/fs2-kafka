/*
 * Copyright 2018 OVO Energy Ltd
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
import fs2.kafka.internal.instances._
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}

sealed abstract class ProducerResult[K, V, P] {
  def passthrough: P
}

object ProducerResult {
  sealed abstract class Single[K, V, P](
    val metadata: RecordMetadata,
    val record: ProducerRecord[K, V],
    override val passthrough: P
  ) extends ProducerResult[K, V, P] {
    override def toString: String =
      s"Single($metadata -> $record, $passthrough)"
  }

  object Single {
    def unapply[K, V, P](
      result: ProducerResult[K, V, P]
    ): Option[(RecordMetadata, ProducerRecord[K, V], P)] = result match {
      case single: Single[K, V, P] => Some((single.metadata, single.record, single.passthrough))
      case _                       => None
    }
  }

  sealed abstract class MultiplePart[K, V](
    val metadata: RecordMetadata,
    val record: ProducerRecord[K, V]
  ) {
    override def toString: String =
      s"$metadata -> $record"
  }

  object MultiplePart {
    def unapply[K, V](
      part: MultiplePart[K, V]
    ): Option[(RecordMetadata, ProducerRecord[K, V])] =
      Some((part.metadata, part.record))

    implicit def multiplePartShow[K, V](
      implicit
      K: Show[K],
      V: Show[V]
    ): Show[MultiplePart[K, V]] = Show.show { mp =>
      show"${mp.metadata} -> ${mp.record}"
    }
  }

  sealed abstract class Multiple[K, V, P](
    val parts: List[MultiplePart[K, V]],
    override val passthrough: P
  ) extends ProducerResult[K, V, P] {
    override def toString: String =
      parts.mkString("Multiple(", ", ", s", $passthrough)")
  }

  object Multiple {
    def unapply[K, V, P](
      result: ProducerResult[K, V, P]
    ): Option[(List[MultiplePart[K, V]], P)] = result match {
      case multiple: Multiple[K, V, P] => Some((multiple.parts, multiple.passthrough))
      case _                           => None
    }
  }

  sealed abstract class Passthrough[K, V, P](
    override val passthrough: P
  ) extends ProducerResult[K, V, P] {
    override def toString: String =
      s"Passthrough($passthrough)"
  }

  object Passthrough {
    def unapply[K, V, P](
      result: ProducerResult[K, V, P]
    ): Option[P] = result match {
      case passthrough: Passthrough[K, V, P] => Some(passthrough.passthrough)
      case _                                 => None
    }
  }

  def single[K, V, P](
    metadata: RecordMetadata,
    record: ProducerRecord[K, V],
    passthrough: P
  ): ProducerResult[K, V, P] =
    new Single(metadata, record, passthrough) {}

  def multiple[K, V, P](
    parts: List[MultiplePart[K, V]],
    passthrough: P
  ): ProducerResult[K, V, P] =
    new Multiple(parts, passthrough) {}

  def multiplePart[K, V](
    metadata: RecordMetadata,
    record: ProducerRecord[K, V]
  ): MultiplePart[K, V] =
    new MultiplePart(metadata, record) {}

  def passthrough[K, V, P](
    passthrough: P
  ): ProducerResult[K, V, P] =
    new Passthrough[K, V, P](passthrough) {}

  implicit def producerResultShow[K, V, P](
    implicit
    K: Show[K],
    V: Show[V],
    P: Show[P]
  ): Show[ProducerResult[K, V, P]] = Show.show {
    case Single(metadata, record, passthrough) =>
      show"Single($metadata -> $record, $passthrough)"
    case Multiple(parts, passthrough) =>
      parts.map(_.show).mkString("Multiple(", ", ", s", $passthrough)")
    case Passthrough(passthrough) =>
      show"Passthrough($passthrough)"
  }
}

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

import cats.syntax.foldable._
import cats.syntax.show._
import cats.{Foldable, Show}
import fs2.kafka.internal.instances._
import fs2.kafka.internal.syntax._
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}

/**
  * [[ProducerResult]] represents the result of having produced zero
  * or more `ProducerRecord`s in the form of [[ProducerMessage]]s,
  * while keeping an arbitrary passthrough value. [[ProducerResult]]s
  * can be created using one of the following options.<br>
  * <br>
  * - `ProducerResult#single` for when exactly one record has been
  * produced using `ProducerMessage#single`.<br>
  * - `ProducerResult#multiple` when zero or more records have been
  * produced with `ProducerMessage#multiple`.<br>
  * - `ProducerResult#passthrough` when exactly zero records have been
  * produced using `ProducerMessage#passthrough`.<br>
  * <br>
  * Most often, only the [[passthrough]] value needs to be accessed.
  * If you need to access the `RecordMetadata` from having produced
  * some records, or the `ProducerRecord`s themselves, then there are
  * also extractors for the three cases: [[ProducerResult#Single]],
  * [[ProducerResult#Multiple]], [[ProducerResult#Passthrough]].
  */
sealed abstract class ProducerResult[+F[_], +K, +V, +P] {
  def passthrough: P
}

object ProducerResult {
  private[this] final class Single[F[_], K, V, P](
    val metadata: RecordMetadata,
    val record: ProducerRecord[K, V],
    override val passthrough: P
  ) extends ProducerResult[F, K, V, P] {
    override def toString: String =
      s"Single($metadata -> $record, $passthrough)"
  }

  object Single {
    def unapply[F[_], K, V, P](
      result: ProducerResult[F, K, V, P]
    ): Option[(RecordMetadata, ProducerRecord[K, V], P)] = result match {
      case single: Single[F, K, V, P] => Some((single.metadata, single.record, single.passthrough))
      case _                          => None
    }
  }

  sealed abstract class MultiplePart[K, V] {
    def metadata: RecordMetadata

    def record: ProducerRecord[K, V]
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

  private[this] final class Multiple[F[_], K, V, P](
    val parts: F[MultiplePart[K, V]],
    override val passthrough: P
  )(implicit F: Foldable[F])
      extends ProducerResult[F, K, V, P] {
    override def toString: String =
      if (parts.isEmpty) s"Multiple(<empty>, $passthrough)"
      else parts.mkString("Multiple(", ", ", s", $passthrough)")
  }

  object Multiple {
    def unapply[F[_], K, V, P](
      result: ProducerResult[F, K, V, P]
    ): Option[(F[MultiplePart[K, V]], P)] = result match {
      case multiple: Multiple[F, K, V, P] => Some((multiple.parts, multiple.passthrough))
      case _                              => None
    }
  }

  private[this] final class Passthrough[F[_], K, V, P](
    override val passthrough: P
  ) extends ProducerResult[F, K, V, P] {
    override def toString: String =
      s"Passthrough($passthrough)"
  }

  object Passthrough {
    def unapply[F[_], K, V, P](
      result: ProducerResult[F, K, V, P]
    ): Option[P] = result match {
      case passthrough: Passthrough[F, K, V, P] => Some(passthrough.passthrough)
      case _                                    => None
    }
  }

  /**
    * Creates a new [[ProducerResult]] for the result of having produced
    * exactly one `ProducerRecord` using `ProducerMessage#single`.
    * [[ProducerResult#Single]] can be used to extract instances
    * created with this function.
    */
  def single[F[_], K, V, P](
    metadata: RecordMetadata,
    record: ProducerRecord[K, V],
    passthrough: P
  ): ProducerResult[F, K, V, P] =
    new Single(metadata, record, passthrough)

  /**
    * Creates a new [[ProducerResult]] for the result of having produced
    * exactly one `ProducerRecord` using `ProducerMessage#single`.
    * [[ProducerResult#Single]] can be used to extract instances
    * created with this function.
    */
  def single[F[_], K, V](
    metadata: RecordMetadata,
    record: ProducerRecord[K, V]
  ): ProducerResult[F, K, V, Unit] =
    single(metadata, record, ())

  /**
    * Creates a new [[ProducerResult]] for the result of having produced
    * zero or more `ProducerRecord`s using `ProducerMessage#multiple`.
    * The parts can be created using [[ProducerResult#multiplePart]].
    * [[ProducerResult#Multiple]] can be used to extract instances
    * created with this function.
    */
  def multiple[F[_], K, V, P](
    parts: F[MultiplePart[K, V]],
    passthrough: P
  )(
    implicit F: Foldable[F]
  ): ProducerResult[F, K, V, P] =
    new Multiple(parts, passthrough)

  /**
    * Creates a new [[ProducerResult]] for the result of having produced
    * zero or more `ProducerRecord`s using `ProducerMessage#multiple`.
    * The parts can be created using [[ProducerResult#multiplePart]].
    * [[ProducerResult#Multiple]] can be used to extract instances
    * created with this function.
    */
  def multiple[F[_], K, V](
    parts: F[MultiplePart[K, V]]
  )(
    implicit F: Foldable[F]
  ): ProducerResult[F, K, V, Unit] =
    multiple(parts, ())

  /**
    * Creates a new [[MultiplePart]] for use with `ProducerResult#multiple`.
    * Each part consists of the `ProducerRecord` and `RecordMetadata` metadata
    * from having produced a single record.
    */
  def multiplePart[K, V](
    metadata: RecordMetadata,
    record: ProducerRecord[K, V]
  ): MultiplePart[K, V] = {
    val _metadata = metadata
    val _record = record

    new MultiplePart[K, V] {
      override val metadata: RecordMetadata = _metadata

      override val record: ProducerRecord[K, V] = _record

      override def toString: String =
        s"${_metadata} -> ${_record}"
    }
  }

  /**
    * Creates a new [[ProducerResult]] for the result of having produced
    * exactly zero `ProducerRecord`s using `ProducerMessage#passthrough`.
    * [[ProducerResult#Passthrough]] can be used to extract instances
    * created with this function.
    */
  def passthrough[F[_], K, V, P](
    passthrough: P
  ): ProducerResult[F, K, V, P] =
    new Passthrough[F, K, V, P](passthrough)

  implicit def producerResultShow[F[_], K, V, P](
    implicit
    F: Foldable[F],
    K: Show[K],
    V: Show[V],
    P: Show[P]
  ): Show[ProducerResult[F, K, V, P]] = Show.show {
    case Single(metadata, record, passthrough) =>
      show"Single($metadata -> $record, $passthrough)"
    case Multiple(parts, passthrough) =>
      if (parts.isEmpty) show"Multiple(<empty>, $passthrough)"
      else parts.mkStringShow("Multiple(", ", ", s", $passthrough)")
    case Passthrough(passthrough) =>
      show"Passthrough($passthrough)"
  }
}

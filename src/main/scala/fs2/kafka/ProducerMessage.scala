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
import org.apache.kafka.clients.producer.ProducerRecord

/**
  * [[ProducerMessage]] represents zero or more `ProducerRecord`s
  * together with an arbitrary passthrough value, which together
  * can be used with [[KafkaProducer]]. A [[ProducerMessage]] can
  * be created using one of the following options.<br>
  * <br>
  * - `ProducerMessage#single` to produce exactly one record and
  * then emit a [[ProducerResult#Single]] with the result and the
  * passthrough value.<br>
  * - `ProducerMessage#multiple` to produce zero or more records
  * and then emit a [[ProducerResult#Multiple]] with the results
  * and the passthrough value.<br>
  * - `ProducerMessage#passthrough` to produce exactly zero records,
  * simply emitting a [[ProducerResult#Passthrough]] with the specified
  * passthrough value.<br>
  * <br>
  * While normally not necessary, the passthrough of a [[ProducerMessage]]
  * can be accessed via [[passthrough]]. There are also extractors for the
  * three cases: [[ProducerMessage#Single]], [[ProducerMessage#Multiple]],
  * and [[ProducerMessage#Passthrough]].
  */
sealed abstract class ProducerMessage[+F[_], +K, +V, +P] {
  def passthrough: P
}

object ProducerMessage {
  sealed abstract class Single[F[_], K, V, P](
    val record: ProducerRecord[K, V],
    override val passthrough: P
  ) extends ProducerMessage[F, K, V, P] {
    override def toString: String =
      s"Single($record, $passthrough)"
  }

  object Single {
    def unapply[F[_], K, V, P](
      message: ProducerMessage[F, K, V, P]
    ): Option[(ProducerRecord[K, V], P)] = message match {
      case single: Single[F, K, V, P] => Some((single.record, single.passthrough))
      case _                          => None
    }
  }

  sealed abstract class Multiple[F[_], K, V, P](
    val records: F[ProducerRecord[K, V]],
    override val passthrough: P
  )(implicit F: Foldable[F])
      extends ProducerMessage[F, K, V, P] {
    override def toString: String =
      if (records.isEmpty) s"Multiple(<empty>, $passthrough)"
      else records.mkString("Multiple(", ", ", s", $passthrough)")
  }

  object Multiple {
    def unapply[F[_], K, V, P](
      message: ProducerMessage[F, K, V, P]
    ): Option[(F[ProducerRecord[K, V]], P)] = message match {
      case multiple: Multiple[F, K, V, P] => Some((multiple.records, multiple.passthrough))
      case _                              => None
    }
  }

  sealed abstract class Passthrough[F[_], K, V, P](
    override val passthrough: P
  ) extends ProducerMessage[F, K, V, P] {
    override def toString: String =
      s"Passthrough($passthrough)"
  }

  object Passthrough {
    def unapply[F[_], K, V, P](
      message: ProducerMessage[F, K, V, P]
    ): Option[P] = message match {
      case passthrough: Passthrough[F, K, V, P] => Some(passthrough.passthrough)
      case _                                    => None
    }
  }

  /**
    * Creates a new [[ProducerMessage]] for producing exactly one
    * `ProducerRecord`, then emitting a [[ProducerResult#Single]]
    * with the result and specified passthrough value.<br>
    * <br>
    * [[ProducerMessage#Single]] can be used to extract instances
    * created with this function.
    */
  def single[F[_], K, V, P](
    record: ProducerRecord[K, V],
    passthrough: P
  ): ProducerMessage[F, K, V, P] =
    new Single(record, passthrough) {}

  /**
    * Creates a new [[ProducerMessage]] for producing exactly one
    * `ProducerRecord`, then emitting a [[ProducerResult#Single]]
    * with the result and `Unit` passthrough value.<br>
    * <br>
    * [[ProducerMessage#Single]] can be used to extract instances
    * created with this function.
    */
  def single[F[_], K, V](
    record: ProducerRecord[K, V]
  ): ProducerMessage[F, K, V, Unit] =
    single(record, ())

  /**
    * Creates a new [[ProducerMessage]] for producing zero or more
    * `ProducerRecords`s, then emitting a [[ProducerResult#Multiple]]
    * with the results and specified passthrough value.<br>
    * <br>
    * [[ProducerMessage#Multiple]] can be used to extract instances
    * created with this function.
    */
  def multiple[F[_], K, V, P](
    records: F[ProducerRecord[K, V]],
    passthrough: P
  )(
    implicit F: Foldable[F]
  ): ProducerMessage[F, K, V, P] =
    new Multiple(records, passthrough) {}

  /**
    * Creates a new [[ProducerMessage]] for producing zero or more
    * `ProducerRecords`s, then emitting a [[ProducerResult#Multiple]]
    * with the results and `Unit` passthrough value.<br>
    * <br>
    * [[ProducerMessage#Multiple]] can be used to extract instances
    * created with this function.
    */
  def multiple[F[_], K, V](
    records: F[ProducerRecord[K, V]]
  )(
    implicit F: Foldable[F]
  ): ProducerMessage[F, K, V, Unit] =
    multiple(records, ())

  /**
    * Creates a new [[ProducerMessage]] for producing exactly zero
    * `ProducerRecord`s, emitting a [[ProducerResult#passthrough]]
    * with the specified passthrough value.<br>
    * <br>
    * [[ProducerMessage#Passthrough]] can be used to extract instances
    * created with this function.
    */
  def passthrough[F[_], K, V, P](
    passthrough: P
  ): ProducerMessage[F, K, V, P] =
    new Passthrough[F, K, V, P](passthrough) {}

  implicit def producerMessageShow[F[_], K, V, P](
    implicit
    F: Foldable[F],
    K: Show[K],
    V: Show[V],
    P: Show[P]
  ): Show[ProducerMessage[F, K, V, P]] = Show.show {
    case Single(record, passthrough) =>
      show"Single($record, $passthrough)"
    case Multiple(records, passthrough) =>
      if (records.isEmpty) show"Multiple(<empty>, $passthrough)"
      else records.mkStringShow("Multiple(", ", ", s", $passthrough)")
    case Passthrough(passthrough) =>
      show"Passthrough($passthrough)"
  }
}

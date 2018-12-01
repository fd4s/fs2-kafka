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
import cats.{Applicative, Foldable, MonoidK, Show, Traverse}
import fs2.kafka.internal.instances._
import fs2.kafka.internal.syntax._
import org.apache.kafka.clients.producer.ProducerRecord

/**
  * [[ProducerMessage]] represents zero or more `ProducerRecord`s,
  * together with an arbitrary passthrough value, all of which can
  * be used with [[KafkaProducer]]. [[ProducerMessage]]s can be
  * created using one of the following options.<br>
  * <br>
  * - `ProducerMessage#single` to produce exactly one record and
  * then emit a [[ProducerResult]] with the result and specified
  * passthrough value.<br>
  * - `ProducerMessage#multiple` to produce zero or more records
  * and then emit a [[ProducerResult]] with the results and
  * specified passthrough value.<br>
  * - `ProducerMessage#passthrough` to produce exactly zero
  * records, only emitting a [[ProducerResult]] with the
  * specified passthrough value.<br>
  * <br>
  * The [[passthrough]] and [[records]] can be retrieved from an
  * existing [[ProducerMessage]] instance.<br>
  * <br>
  * For a [[ProducerMessage]] to be usable by [[KafkaProducer]],
  * it needs a `Traverse` instance. This requirement is captured
  * in [[ProducerMessage]] via [[traverse]].
  */
sealed abstract class ProducerMessage[F[_], K, V, +P] {

  /** The records to produce. Can be empty for passthrough-only. */
  def records: F[ProducerRecord[K, V]]

  /** The passthrough to emit once all [[records]] have been produced. */
  def passthrough: P

  /** The traverse instance for `F[_]`. Required by [[KafkaProducer]]. */
  def traverse: Traverse[F]
}

object ProducerMessage {
  private[this] final class ProducerMessageImpl[F[_], K, V, +P](
    override val records: F[ProducerRecord[K, V]],
    override val passthrough: P,
    override val traverse: Traverse[F]
  ) extends ProducerMessage[F, K, V, P] {
    override def toString: String = {
      implicit val F: Foldable[F] = traverse
      if (records.isEmpty) s"ProducerMessage(<empty>, $passthrough)"
      else records.mkString("ProducerMessage(", ", ", s", $passthrough)")
    }
  }

  /**
    * Creates a new [[ProducerMessage]] for producing exactly one
    * `ProducerRecord`, then emitting a [[ProducerResult]] with
    * the result and specified passthrough value.
    */
  def single[F[_], K, V, P](
    record: ProducerRecord[K, V],
    passthrough: P
  )(
    implicit F: Traverse[F],
    A: Applicative[F]
  ): ProducerMessage[F, K, V, P] =
    multiple(A.pure(record), passthrough)

  /**
    * Creates a new [[ProducerMessage]] for producing exactly one
    * `ProducerRecord`, then emitting a [[ProducerResult]] with
    * the result and `Unit` passthrough value.
    */
  def single[F[_], K, V](
    record: ProducerRecord[K, V]
  )(
    implicit F: Traverse[F],
    A: Applicative[F]
  ): ProducerMessage[F, K, V, Unit] =
    single(record, ())

  /**
    * Creates a new [[ProducerMessage]] for producing zero or more
    * `ProducerRecords`s, then emitting a [[ProducerResult]] with
    * the results and specified passthrough value.
    */
  def multiple[F[_], K, V, P](
    records: F[ProducerRecord[K, V]],
    passthrough: P
  )(
    implicit F: Traverse[F]
  ): ProducerMessage[F, K, V, P] =
    new ProducerMessageImpl(records, passthrough, F)

  /**
    * Creates a new [[ProducerMessage]] for producing zero or more
    * `ProducerRecords`s, then emitting a [[ProducerResult]] with
    * the results and `Unit` passthrough value.
    */
  def multiple[F[_], K, V](
    records: F[ProducerRecord[K, V]]
  )(
    implicit F: Traverse[F]
  ): ProducerMessage[F, K, V, Unit] =
    multiple(records, ())

  /**
    * Creates a new [[ProducerMessage]] for producing exactly zero
    * `ProducerRecord`s, emitting a [[ProducerResult]] with the
    * specified passthrough value.
    */
  def passthrough[F[_], K, V, P](
    passthrough: P
  )(
    implicit F: Traverse[F],
    M: MonoidK[F]
  ): ProducerMessage[F, K, V, P] =
    multiple(M.empty, passthrough)

  implicit def producerMessageShow[F[_], K, V, P](
    implicit
    K: Show[K],
    V: Show[V],
    P: Show[P]
  ): Show[ProducerMessage[F, K, V, P]] = Show.show { message =>
    implicit val F: Foldable[F] = message.traverse
    if (message.records.isEmpty) show"ProducerMessage(<empty>, ${message.passthrough})"
    else message.records.mkStringShow("ProducerMessage(", ", ", s", ${message.passthrough})")
  }
}

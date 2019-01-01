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
    * Enables creating [[ProducerMessage]]s with the following syntax.
    *
    * {{{
    * ProducerMessage.single[F].of(record, passthrough)
    *
    * ProducerMessage.single[F].of(record)
    * }}}
    */
  final class SingleApplyBuilders[F[_]] private[kafka] (
    private val F: Traverse[F]
  ) extends AnyVal {

    /**
      * Creates a new [[ProducerMessage]] using the specified
      * `ProducerRecord` and specified passthrough value.
      */
    def of[K, V, P](
      record: ProducerRecord[K, V],
      passthrough: P
    )(
      implicit A: Applicative[F]
    ): ProducerMessage[F, K, V, P] =
      ProducerMessage.single(record, passthrough)(F, A)

    /**
      * Creates a new [[ProducerMessage]] using the specified
      * `ProducerRecord` and `Unit` passthrough value.
      */
    def of[K, V](
      record: ProducerRecord[K, V]
    )(
      implicit A: Applicative[F]
    ): ProducerMessage[F, K, V, Unit] =
      ProducerMessage.single(record)(F, A)
  }

  /**
    * Enables creating [[ProducerMessage]]s with the following syntax.
    *
    * {{{
    * ProducerMessage.multiple[F].of(records, passthrough)
    *
    * ProducerMessage.multiple[F].of(records)
    * }}}
    */
  final class MultipleApplyBuilders[F[_]] private[kafka] (
    private val F: Traverse[F]
  ) extends AnyVal {

    /**
      * Creates a new [[ProducerMessage]] using the specified
      * `ProducerRecord`s and specified passthrough value.
      */
    def of[K, V, P](
      records: F[ProducerRecord[K, V]],
      passthrough: P
    ): ProducerMessage[F, K, V, P] =
      ProducerMessage.multiple(records, passthrough)(F)

    /**
      * Creates a new [[ProducerMessage]] using the specified
      * `ProducerRecord`s and `Unit` passthrough value.
      */
    def of[K, V](
      records: F[ProducerRecord[K, V]]
    ): ProducerMessage[F, K, V, Unit] =
      ProducerMessage.multiple(records)(F)
  }

  /**
    * Enables creating [[ProducerMessage]]s with the following syntax.
    *
    * {{{
    * ProducerMessage.passthrough[F].withKeyAndValue[K, V].of(passthrough)
    * }}}
    */
  final class PassthroughKeyAndValueApplyBuilders[F[_], K, V] private[kafka] (
    private val F: Traverse[F]
  ) extends AnyVal {

    /**
      * Creates a new [[ProducerMessage]] using the specified
      * passthrough value.
      */
    def of[P](
      passthrough: P
    )(
      implicit M: MonoidK[F]
    ): ProducerMessage[F, K, V, P] =
      ProducerMessage.passthrough(passthrough)(F, M)
  }

  /**
    * Enables creating [[ProducerMessage]]s with the following syntax.
    *
    * {{{
    * ProducerMessage.passthrough[F].of(passthrough)
    *
    * ProducerMessage.passthrough[F].withKeyAndValue[K, V].of(passthrough)
    * }}}
    */
  final class PassthroughApplyBuilders[F[_]] private[kafka] (
    private val F: Traverse[F]
  ) extends AnyVal {

    /**
      * Creates a new [[ProducerMessage]] using the specified
      * passthrough value.
      */
    def of[K, V, P](
      passthrough: P
    )(
      implicit M: MonoidK[F]
    ): ProducerMessage[F, K, V, P] =
      ProducerMessage.passthrough(passthrough)(F, M)

    /**
      * Creates a new [[ProducerMessage]] by first explicitly
      * specifying the key and value types. This allows you
      * to use the following syntax.
      *
      * {{{
      * ProducerMessage.passthrough[F].withKeyAndValue[K, V].of(passthrough)
      * }}}
      */
    def withKeyAndValue[K, V]: PassthroughKeyAndValueApplyBuilders[F, K, V] =
      new PassthroughKeyAndValueApplyBuilders[F, K, V](F)
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
    * Creates a new [[ProducerMessage]] for producing exactly one
    * `ProducerRecord`, then emitting a [[ProducerResult]] with
    * the result and either specified or `Unit` passthrough.<br>
    * <br>
    * This version allows you to explicitly specify the context `F[_]`,
    * while the key, value, and passthrough types can be inferred. This
    * is useful when the context cannot otherwise be inferred correctly.<br>
    * <br>
    * This function enables the following syntax.
    *
    * {{{
    * ProducerMessage.single[F].of(record, passthrough)
    *
    * ProducerMessage.single[F].of(record)
    * }}}
    */
  def single[F[_]](implicit F: Traverse[F]): SingleApplyBuilders[F] =
    new SingleApplyBuilders[F](F)

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
    * Creates a new [[ProducerMessage]] for producing zero or more
    * `ProducerRecord`s, then emitting a [[ProducerResult]] with
    * the results and either specified or `Unit` passthrough.<br>
    * <br>
    * This version allows you to explicitly specify the context `F[_]`,
    * while the key, value, and passthrough types can be inferred. This
    * is useful when the context cannot otherwise be inferred correctly.<br>
    * <br>
    * This function enables the following syntax.
    *
    * {{{
    * ProducerMessage.multiple[F].of(records, passthrough)
    *
    * ProducerMessage.multiple[F].of(records)
    * }}}
    */
  def multiple[F[_]](implicit F: Traverse[F]): MultipleApplyBuilders[F] =
    new MultipleApplyBuilders[F](F)

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

  /**
    * Creates a new [[ProducerMessage]] for producing exactly zero
    * `ProducerRecord`s, then emitting a [[ProducerResult]] with
    * the specified passthrough value.<br>
    * <br>
    * This version allows you to explicitly specify the context `F[_]`,
    * while the key, value, and passthrough types can be inferred. This
    * is useful when the context cannot otherwise be inferred correctly.
    * If needed, the key and value types can be explicitly specified by
    * also using `withKeyAndValue`.<br>
    * <br>
    * This function enables the following syntax.
    *
    * {{{
    * ProducerMessage.passthrough[F].of(passthrough)
    *
    * ProducerMessage.passthrough[F].withKeyAndValue[K, V].of(passthrough)
    * }}}
    */
  def passthrough[F[_]](implicit F: Traverse[F]): PassthroughApplyBuilders[F] =
    new PassthroughApplyBuilders[F](F)

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

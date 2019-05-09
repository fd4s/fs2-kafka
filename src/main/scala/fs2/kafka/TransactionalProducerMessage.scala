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

import cats._
import cats.implicits._
import fs2.kafka.internal.syntax._

/**
  * [[TransactionalProducerMessage]] represents zero or more `ProducerRecord`s
  * paired with corresponding `CommittableOffset`s, together with an arbitrary
  * passthrough value, all of which can be used with [[TransactionalKafkaProducer]].
  * [[TransactionalProducerMessage]]s can be created using one of the following options.<br>
  * <br>
  * - `TransactionalMessage#apply` to produce zero or more records
  * and then emit a [[ProducerResult]] with the results and
  * specified passthrough value.<br>
  * - `TransactionalMessage#one` to produce exactly one record and
  * then emit a [[ProducerResult]] with the result and specified
  * passthrough value.<br>
  * <br>
  * The [[passthrough]] and [[records]] can be retrieved from an
  * existing [[TransactionalProducerMessage]] instance.<br>
  * <br>
  * For a [[TransactionalProducerMessage]] to be usable by [[TransactionalKafkaProducer]],
  * it needs a `Traverse[F]` instance. This requirement is
  * captured in [[TransactionalProducerMessage]] as [[traverse]].
  */
sealed abstract class TransactionalProducerMessage[F[_], G[+ _], +K, +V, +P] {

  def records: G[ProducerRecord[K, V]]

  def offsetBatch: CommittableOffsetBatch[F]

  def consumerGroupId: String

  def passthrough: P

  def traverse: Traverse[G]
}

object TransactionalProducerMessage {
  private[this] final class TransactionalProducerMessageImpl[F[_], G[+ _], +K, +V, +P](
    override val records: G[ProducerRecord[K, V]],
    override val offsetBatch: CommittableOffsetBatch[F],
    override val consumerGroupId: String,
    override val passthrough: P,
    override val traverse: NonEmptyTraverse[G]
  ) extends TransactionalProducerMessage[F, G, K, V, P] {
    override def toString: String = {
      implicit val G: Foldable[G] = traverse
      records.mkString(
        "TransactionalProducerMessage(",
        ", ",
        s", $offsetBatch, $consumerGroupId, $passthrough)"
      )
    }
  }

  /**
    * Enables creating [[TransactionalProducerMessage]]s with the following syntax.
    *
    * {{{
    * TransactionalMessage[G].of(records, passthrough)
    *
    * TransactionalMessage[G].of(records)
    * }}}
    */
  final class ApplyBuilders[G[+ _]] private[kafka] (
    private val G: NonEmptyTraverse[G]
  ) extends AnyVal {

    /**
      * Creates a new [[TransactionalProducerMessage]] for producing zero or more
      * `ProducerRecord`s within a Kafka transaction, then emitting a
      * [[ProducerResult]] with the results and specified passthrough value.
      */
    def of[F[_], K, V, P](
      records: G[(ProducerRecord[K, V], CommittableOffset[F])],
      passthrough: P
    )(implicit F: MonadError[F, Throwable]): F[TransactionalProducerMessage[F, G, K, V, P]] =
      TransactionalProducerMessage(records, passthrough)(F, G)

    /**
      * Creates a new [[TransactionalProducerMessage]] for producing zero or more
      * `ProducerRecord`s within a Kafka transaction, then emitting a
      * [[ProducerResult]] with the results and `Unit` passthrough value.
      */
    def of[F[_], K, V](
      records: G[(ProducerRecord[K, V], CommittableOffset[F])]
    )(implicit F: MonadError[F, Throwable]): F[TransactionalProducerMessage[F, G, K, V, Unit]] =
      TransactionalProducerMessage(records)(F, G)
  }

  /**
    * Creates a new [[TransactionalProducerMessage]] for producing exactly one
    * `ProducerRecord` within a Kafka transaction, then emitting a
    * [[ProducerResult]] with the results and specified passthrough value.
    */
  def one[F[_], K, V, P](
    record: (ProducerRecord[K, V], CommittableOffset[F]),
    passthrough: P
  )(implicit F: MonadError[F, Throwable]): F[TransactionalProducerMessage[F, Id, K, V, P]] =
    apply[F, Id, K, V, P](record, passthrough)

  /**
    * Creates a new [[TransactionalProducerMessage]] for producing exactly one
    * `ProducerRecord` within a Kafka transaction, then emitting a
    * [[ProducerResult]] with the results and `Unit` passthrough value.
    */
  def one[F[_], K, V](
    record: (ProducerRecord[K, V], CommittableOffset[F])
  )(implicit F: MonadError[F, Throwable]): F[TransactionalProducerMessage[F, Id, K, V, Unit]] =
    one(record, ())

  /**
    * Creates a new [[TransactionalProducerMessage]] for producing zero or more
    * `ProducerRecord`s within a Kafka transaction, then emitting a
    * [[ProducerResult]] with the results and specified passthrough value.
    */
  def apply[F[_], G[+ _], K, V, P](
    records: G[(ProducerRecord[K, V], CommittableOffset[F])],
    passthrough: P
  )(
    implicit F: MonadError[F, Throwable],
    G: NonEmptyTraverse[G]
  ): F[TransactionalProducerMessage[F, G, K, V, P]] = {
    val consumerGroups = records.foldMap {
      case (_, offset) => Set(offset.consumerGroupId)
    }

    if (consumerGroups.size != 1) {
      F.raiseError(new RuntimeException("multiple consumer group IDs given in transaction"))
    } else {
      consumerGroups.head.fold(
        F.raiseError[TransactionalProducerMessage[F, G, K, V, P]](
          new RuntimeException("no consumer group ID given for transaction")
        )
      ) { id =>
        val offsetBatch = CommittableOffsetBatch.fromFoldable(records.map(_._2))
        F.pure(
          new TransactionalProducerMessageImpl(records.map(_._1), offsetBatch, id, passthrough, G)
        )
      }
    }
  }

  /**
    * Creates a new [[TransactionalProducerMessage]] for producing zero or more
    * `ProducerRecord`s within a Kafka transaction, then emitting a
    * [[ProducerResult]] with the results and `Unit` passthrough value.
    */
  def apply[F[_], G[+ _], K, V](
    records: G[(ProducerRecord[K, V], CommittableOffset[F])]
  )(
    implicit F: MonadError[F, Throwable],
    G: NonEmptyTraverse[G]
  ): F[TransactionalProducerMessage[F, G, K, V, Unit]] =
    apply(records, ())

  /**
    * Creates a new [[TransactionalProducerMessage]] for producing zero or more
    * `ProducerRecord`s within a Kafka transaction, then emitting a
    * [[ProducerResult]] with the results and a specified or `Unit`
    * passthrough value.<br>
    * <br>
    * This version allows you to explicitly specify the context `G[_]`.
    * This is useful when the context cannot otherwise be inferred correctly.<br>
    * <br>
    * This function enables the following syntax.
    *
    * {{{
    * TransactionalMessage[G].of(records, passthrough)
    *
    * TransactionalMessage[G].of(records)
    * }}}
    */
  def apply[G[+ _]](implicit G: NonEmptyTraverse[G]): ApplyBuilders[G] =
    new ApplyBuilders[G](G)

  implicit def transactionalMessageShow[F[_], G[+ _], K, V, P](
    implicit
    K: Show[K],
    V: Show[V],
    P: Show[P]
  ): Show[TransactionalProducerMessage[F, G, K, V, P]] = Show.show { message =>
    implicit val G: Foldable[G] = message.traverse
    message.records.mkStringShow(
      "TransactionalProducerMessage(",
      ", ",
      show", ${message.offsetBatch}, ${message.consumerGroupId}, ${message.passthrough})"
    )
  }
}

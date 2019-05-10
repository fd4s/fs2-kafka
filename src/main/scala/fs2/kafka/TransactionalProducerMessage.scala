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
import fs2.kafka.internal.instances._
import fs2.kafka.internal.syntax._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

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
sealed abstract class TransactionalProducerMessage[G[+ _], +K, +V, +P] {

  /**
    * The records to produce within the transaction. If these records are
    * produced, but [[offsets]] fail to be committed, consumers configured
    * to have isolation level "read_committed" will not see the records.
    */
  def records: G[ProducerRecord[K, V]]

  /** Offsets to commit within the transaction where [[records]] are produced. */
  def offsets: Map[TopicPartition, OffsetAndMetadata]

  /** Group ID of the consumer which pulled `offsets` from Kafka. */
  def consumerGroupId: String

  /**
    * The passthrough to emit once all [[records]] have been produced,
    * [[offsets]] have been committed, and the wrapping transaction has
    * been completed.
    */
  def passthrough: P

  /** The traverse instance for `F[_]`. Required by [[TransactionalKafkaProducer]]. */
  def traverse: Traverse[G]
}

object TransactionalProducerMessage {
  private[this] final class TransactionalProducerMessageImpl[G[+ _], +K, +V, +P](
    override val records: G[ProducerRecord[K, V]],
    override val offsets: Map[TopicPartition, OffsetAndMetadata],
    override val consumerGroupId: String,
    override val passthrough: P,
    override val traverse: NonEmptyTraverse[G]
  ) extends TransactionalProducerMessage[G, K, V, P] {
    override def toString: String = {
      implicit val G: Foldable[G] = traverse
      val offsetStr =
        if (offsets.isEmpty) {
          "<empty>"
        } else {
          offsets.toList.sorted.map {
            case (tp, oam) => s"${tp.show} -> ${oam.show}"
          }.mkString("[", ", ", "]")
        }

      records.mkString(
        "TransactionalProducerMessage(",
        ", ",
        s", $offsetStr, $consumerGroupId, $passthrough)"
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
      records: G[ProducerRecord[K, V]],
      offsetBatch: CommittableOffsetBatch[F],
      passthrough: P
    )(implicit F: MonadError[F, Throwable]): F[TransactionalProducerMessage[G, K, V, P]] =
      TransactionalProducerMessage(records, offsetBatch, passthrough)(F, G)

    /**
      * Creates a new [[TransactionalProducerMessage]] for producing zero or more
      * `ProducerRecord`s within a Kafka transaction, then emitting a
      * [[ProducerResult]] with the results and `Unit` passthrough value.
      */
    def of[F[_], K, V](
      records: G[ProducerRecord[K, V]],
      offsetBatch: CommittableOffsetBatch[F]
    )(implicit F: MonadError[F, Throwable]): F[TransactionalProducerMessage[G, K, V, Unit]] =
      TransactionalProducerMessage(records, offsetBatch)(F, G)
  }

  /**
    * Creates a new [[TransactionalProducerMessage]] for producing exactly one
    * `ProducerRecord` within a Kafka transaction, then emitting a
    * [[ProducerResult]] with the results and specified passthrough value.
    */
  def one[F[_], K, V, P](
    record: ProducerRecord[K, V],
    offsetBatch: CommittableOffsetBatch[F],
    passthrough: P
  )(implicit F: MonadError[F, Throwable]): F[TransactionalProducerMessage[Id, K, V, P]] =
    apply[F, Id, K, V, P](record, offsetBatch, passthrough)

  /**
    * Creates a new [[TransactionalProducerMessage]] for producing exactly one
    * `ProducerRecord` within a Kafka transaction, then emitting a
    * [[ProducerResult]] with the results and `Unit` passthrough value.
    */
  def one[F[_], K, V](
    record: ProducerRecord[K, V],
    offsetBatch: CommittableOffsetBatch[F]
  )(implicit F: MonadError[F, Throwable]): F[TransactionalProducerMessage[Id, K, V, Unit]] =
    one(record, offsetBatch, ())

  /**
    * Creates a new [[TransactionalProducerMessage]] for producing zero or more
    * `ProducerRecord`s within a Kafka transaction, then emitting a
    * [[ProducerResult]] with the results and specified passthrough value.
    */
  def apply[F[_], G[+ _], K, V, P](
    records: G[ProducerRecord[K, V]],
    offsetBatch: CommittableOffsetBatch[F],
    passthrough: P
  )(
    implicit F: MonadError[F, Throwable],
    G: NonEmptyTraverse[G]
  ): F[TransactionalProducerMessage[G, K, V, P]] =
    F.whenA(offsetBatch.consumerGroupIds.size != 1) {
        F.raiseError(
          new RuntimeException(
            "Transactional messages must contain messages from exactly one consuer group"
          )
        )
      }
      .map { _ =>
        new TransactionalProducerMessageImpl(
          records,
          offsetBatch.offsets,
          offsetBatch.consumerGroupIds.head,
          passthrough,
          G
        )
      }

  /**
    * Creates a new [[TransactionalProducerMessage]] for producing zero or more
    * `ProducerRecord`s within a Kafka transaction, then emitting a
    * [[ProducerResult]] with the results and `Unit` passthrough value.
    */
  def apply[F[_], G[+ _], K, V](
    records: G[ProducerRecord[K, V]],
    offsetBatch: CommittableOffsetBatch[F]
  )(
    implicit F: MonadError[F, Throwable],
    G: NonEmptyTraverse[G]
  ): F[TransactionalProducerMessage[G, K, V, Unit]] =
    apply(records, offsetBatch, ())

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

  implicit def transactionalMessageShow[G[+ _], K, V, P](
    implicit
    K: Show[K],
    V: Show[V],
    P: Show[P]
  ): Show[TransactionalProducerMessage[G, K, V, P]] = Show.show { message =>
    implicit val G: Foldable[G] = message.traverse
    val offsets =
      if (message.offsets.isEmpty) {
        "<empty>"
      } else {
        message.offsets.toList.sorted.mkStringAppend {
          case (append, (tp, oam)) =>
            append(tp.show)
            append(" -> ")
            append(oam.show)
        }(
          start = "[",
          sep = ", ",
          end = "]"
        )
      }

    message.records.mkStringShow(
      "TransactionalProducerMessage(",
      ", ",
      show", $offsets, ${message.consumerGroupId}, ${message.passthrough})"
    )
  }
}

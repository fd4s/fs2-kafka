/*
 * Copyright 2018-2020 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.Show
import cats.syntax.show._

/**
  * [[CommittableConsumerRecord]] is a Kafka record along with an
  * instance of [[CommittableOffset]], which can be used commit
  * the record offset to Kafka. Offsets are normally committed in
  * batches, either using [[CommittableOffsetBatch]] or via pipes,
  * like [[commitBatchWithin]]. If you are not committing offsets
  * to Kafka then you can use [[record]] to get the underlying
  * record and also discard the [[offset]].<br>
  * <br>
  * While normally not necessary, [[CommittableConsumerRecord#apply]]
  * can be used to create a new instance.
  */
sealed abstract class CommittableConsumerRecord[F[_], +K, +V] {

  /**
    * The Kafka record for the [[CommittableConsumerRecord]]. If you
    * are not committing offsets to Kafka, simply use this to get the
    * [[ConsumerRecord]] and discard the [[offset]].
    */
  def record: ConsumerRecord[K, V]

  /**
    * A [[CommittableOffset]] instance, providing a way to commit the
    * [[record]] offset to Kafka. This is normally done in batches as
    * it achieves better performance. Pipes like [[commitBatchWithin]]
    * use [[CommittableOffsetBatch]] to batch and commit offsets.
    */
  def offset: CommittableOffset[F]
}

object CommittableConsumerRecord {
  private[this] final class CommittableConsumerRecordImpl[F[_], +K, +V](
    override val record: ConsumerRecord[K, V],
    override val offset: CommittableOffset[F]
  ) extends CommittableConsumerRecord[F, K, V] {
    override def toString: String =
      s"CommittableConsumerRecord($record, $offset)"
  }

  /**
    * Creates a new [[CommittableConsumerRecord]] using the specified
    * Kafka [[ConsumerRecord]] and [[CommittableOffset]], which can
    * be used to commit the record offset to Kafka.
    */
  def apply[F[_], K, V](
    record: ConsumerRecord[K, V],
    offset: CommittableOffset[F]
  ): CommittableConsumerRecord[F, K, V] =
    new CommittableConsumerRecordImpl(record, offset)

  def unapply[F[_], K, V](
    ccr: CommittableConsumerRecord[F, K, V]
  ): Some[(ConsumerRecord[K, V], CommittableOffset[F])] =
    Some((ccr.record, ccr.offset))

  implicit def committableConsumerRecordShow[F[_], K, V](
    implicit
    K: Show[K],
    V: Show[V]
  ): Show[CommittableConsumerRecord[F, K, V]] = Show.show { cm =>
    show"CommittableConsumerRecord(${cm.record}, ${cm.offset})"
  }
}

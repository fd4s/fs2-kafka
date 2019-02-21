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

import cats.Show
import cats.syntax.show._

/**
  * [[CommittableMessage]] is a Kafka record along with an instance of
  * [[CommittableOffset]], which can be used commit the record offset
  * to Kafka. Offsets are normally committed in batches, either using
  * [[CommittableOffsetBatch]] or via pipes, like [[commitBatch]] and
  * [[commitBatchWithin]]. If you are not committing offsets to Kafka
  * then you can use [[record]] to get the underlying record and also
  * discard the [[committableOffset]].<br>
  * <br>
  * While normally not necessary, [[CommittableMessage#apply]] can be
  * used to create a new instance.
  */
sealed abstract class CommittableMessage[F[_], +K, +V] {

  /**
    * The Kafka record for the [[CommittableMessage]]. If you are not
    * committing offsets to Kafka, simply use this to get the records
    * and discard the [[committableOffset]]s.
    */
  def record: ConsumerRecord[K, V]

  /**
    * A [[CommittableOffset]] instance, providing a way to commit the
    * [[record]] offset to Kafka. This is normally done in batches as
    * it achieves better performance. Pipes like [[commitBatch]] and
    * [[commitBatchWithin]] use [[CommittableOffsetBatch]] to batch
    * and commit offsets.
    */
  def committableOffset: CommittableOffset[F]
}

object CommittableMessage {
  private[this] final class CommittableMessageImpl[F[_], +K, +V](
    override val record: ConsumerRecord[K, V],
    override val committableOffset: CommittableOffset[F]
  ) extends CommittableMessage[F, K, V] {
    override def toString: String =
      s"CommittableMessage($record, $committableOffset)"
  }

  /**
    * Creates a new [[CommittableMessage]] using the specified Kafka
    * record and [[CommittableOffset]], which can be used to commit
    * the record offset to Kafka.
    */
  def apply[F[_], K, V](
    record: ConsumerRecord[K, V],
    committableOffset: CommittableOffset[F]
  ): CommittableMessage[F, K, V] =
    new CommittableMessageImpl(
      record = record,
      committableOffset = committableOffset
    )

  implicit def committableMessageShow[F[_], K, V](
    implicit
    K: Show[K],
    V: Show[V]
  ): Show[CommittableMessage[F, K, V]] = Show.show { cm =>
    show"CommittableMessage(${cm.record}, ${cm.committableOffset})"
  }
}

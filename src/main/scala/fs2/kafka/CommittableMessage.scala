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
import org.apache.kafka.clients.consumer.ConsumerRecord

sealed abstract class CommittableMessage[F[_], K, V] {
  def record: ConsumerRecord[K, V]

  def committableOffset: CommittableOffset[F]
}

object CommittableMessage {
  private[this] final class CommittableMessageImpl[F[_], K, V](
    override val record: ConsumerRecord[K, V],
    override val committableOffset: CommittableOffset[F]
  ) extends CommittableMessage[F, K, V] {
    override def toString: String =
      s"CommittableMessage($record, $committableOffset)"
  }

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

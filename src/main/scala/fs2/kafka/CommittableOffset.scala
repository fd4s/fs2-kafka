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
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

sealed abstract class CommittableOffset[F[_]] {
  def topicPartition: TopicPartition

  def offsetAndMetadata: OffsetAndMetadata

  def offsets: Map[TopicPartition, OffsetAndMetadata]

  def batch: CommittableOffsetBatch[F]

  def commit: F[Unit]
}

object CommittableOffset {
  def apply[F[_]](
    topicPartition: TopicPartition,
    offsetAndMetadata: OffsetAndMetadata,
    commit: Map[TopicPartition, OffsetAndMetadata] => F[Unit]
  ): CommittableOffset[F] = {
    val _topicPartition = topicPartition
    val _offsetAndMetadata = offsetAndMetadata
    val _commit = commit

    new CommittableOffset[F] {
      override val topicPartition: TopicPartition =
        _topicPartition

      override val offsetAndMetadata: OffsetAndMetadata =
        _offsetAndMetadata

      override val offsets: Map[TopicPartition, OffsetAndMetadata] =
        Map(_topicPartition -> _offsetAndMetadata)

      override def batch: CommittableOffsetBatch[F] =
        CommittableOffsetBatch(offsets, _commit)

      override def commit: F[Unit] =
        _commit(offsets)

      override def toString: String =
        Show[CommittableOffset[F]].show(this)
    }
  }

  implicit def committableOffsetShow[F[_]]: Show[CommittableOffset[F]] =
    Show.show(co => show"CommittableOffset(${co.topicPartition} -> ${co.offsetAndMetadata})")
}

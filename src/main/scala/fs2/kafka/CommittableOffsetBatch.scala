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
import cats.{Applicative, Foldable, Show}
import fs2.kafka.internal.instances._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

/**
  * [[CommittableOffsetBatch]] represents a batch of Kafka [[offsets]]
  * which can be committed together using [[commit]]. An offset, or one
  * more batch, can be added an existing batch using `updated`. Note that
  * this requires the offsets per topic-partition to be included in-order,
  * since offset commits in general require it.<br>
  * <br>
  * Use [[CommittableOffsetBatch#empty]] to create an empty batch. The
  * [[CommittableOffset#batch]] function can be used to create a batch
  * from an existing [[CommittableOffset]].<br>
  * <br>
  * If you have some offsets in-order per topic-partition, you can fold
  * them together using [[CommittableOffsetBatch#empty]] and `updated`,
  * or you can use [[CommittableOffsetBatch#fromFoldable]]. Generally,
  * prefer to use `fromFoldable`, as it has better performance. Provided
  * sinks like [[commitBatch]] and [[commitBatchWithin]] are also to be
  * preferred, as they also achieve better performance.
  */
sealed abstract class CommittableOffsetBatch[F[_]] {

  /**
    * Creates a new [[CommittableOffsetBatch]] with the specified offset
    * included. Note that this function requires offsets to be in-order
    * per topic-partition, as provided offsets will override existing
    * offsets for the same topic-partition.
    */
  def updated(that: CommittableOffset[F]): CommittableOffsetBatch[F]

  /**
    * Creates a new [[CommittableOffsetBatch]] with the specified offsets
    * included. Note that this function requires offsets to be in-order
    * per topic-partition, as provided offsets will override existing
    * offsets for the same topic-partition.
    */
  def updated(that: CommittableOffsetBatch[F]): CommittableOffsetBatch[F]

  /**
    * The offsets included in the [[CommittableOffsetBatch]].
    */
  def offsets: Map[TopicPartition, OffsetAndMetadata]

  /**
    * Commits the [[offsets]] to Kafka in a single commit.
    */
  def commit: F[Unit]
}

object CommittableOffsetBatch {
  private[kafka] def apply[F[_]](
    offsets: Map[TopicPartition, OffsetAndMetadata],
    commit: Map[TopicPartition, OffsetAndMetadata] => F[Unit]
  ): CommittableOffsetBatch[F] = {
    val _offsets = offsets
    val _commit = commit

    new CommittableOffsetBatch[F] {
      override def updated(that: CommittableOffset[F]): CommittableOffsetBatch[F] =
        CommittableOffsetBatch(
          _offsets.updated(that.topicPartition, that.offsetAndMetadata),
          _commit
        )

      override def updated(that: CommittableOffsetBatch[F]): CommittableOffsetBatch[F] =
        CommittableOffsetBatch(
          _offsets ++ that.offsets,
          _commit
        )

      override val offsets: Map[TopicPartition, OffsetAndMetadata] =
        _offsets

      override def commit: F[Unit] =
        _commit(offsets)

      override def toString: String =
        Show[CommittableOffsetBatch[F]].show(this)
    }
  }

  /**
    * Creates a [[CommittableOffsetBatch]] from some [[CommittableOffset]]s,
    * where the containing type has a `Foldable` instance. Guaranteed to be
    * equivalent to the following, but implemented more efficiently.
    *
    * {{{
    * offsets.foldLeft(CommittableOffsetBatch.empty[F])(_ updated _)
    * }}}
    *
    * Note that just like for `updated`, `offsets` have to be in order
    * per topic-partition.
    *
    * @see [[CommittableOffsetBatch#fromFoldableOption]]
    */
  def fromFoldable[F[_], G[_]](offsets: G[CommittableOffset[F]])(
    implicit F: Applicative[F],
    G: Foldable[G]
  ): CommittableOffsetBatch[F] = {
    var commit: Map[TopicPartition, OffsetAndMetadata] => F[Unit] = null
    var offsetsMap: Map[TopicPartition, OffsetAndMetadata] = Map.empty
    var empty: Boolean = true

    offsets.foldLeft(()) { (_, offset) =>
      if (empty) { commit = offset.commitOffsets; empty = false }
      offsetsMap = offsetsMap.updated(offset.topicPartition, offset.offsetAndMetadata)
    }

    if (empty) CommittableOffsetBatch.empty[F]
    else CommittableOffsetBatch(offsetsMap, commit)
  }

  /**
    * Creates a [[CommittableOffsetBatch]] from some [[CommittableOffset]]s wrapped
    * in `Option`, where the containing type has a `Foldable` instance. Guaranteed
    * to be equivalent to the following, but implemented more efficiently.
    *
    * {{{
    * offsets.foldLeft(CommittableOffsetBatch.empty[F]) {
    *   case (batch, Some(offset)) => batch.updated(offset)
    *   case (batch, None)         => batch
    * }
    * }}}
    *
    * Note that just like for `updated`, `offsets` have to be in order
    * per topic-partition.
    *
    * @see [[CommittableOffsetBatch#fromFoldable]]
    */
  def fromFoldableOption[F[_], G[_]](offsets: G[Option[CommittableOffset[F]]])(
    implicit F: Applicative[F],
    G: Foldable[G]
  ): CommittableOffsetBatch[F] = {
    var commit: Map[TopicPartition, OffsetAndMetadata] => F[Unit] = null
    var offsetsMap: Map[TopicPartition, OffsetAndMetadata] = Map.empty
    var empty: Boolean = true

    offsets.foldLeft(()) {
      case (_, Some(offset)) =>
        if (empty) { commit = offset.commitOffsets; empty = false }
        offsetsMap = offsetsMap.updated(offset.topicPartition, offset.offsetAndMetadata)
      case (_, None) => ()
    }

    if (empty) CommittableOffsetBatch.empty[F]
    else CommittableOffsetBatch(offsetsMap, commit)
  }

  /**
    * An empty [[CommittableOffsetBatch]] which does not include any
    * offsets and `commit` will not commit offsets. This can be used
    * together with `updated` to create a batch from some offsets.
    *
    * @see [[CommittableOffsetBatch#fromFoldable]]
    * @see [[CommittableOffsetBatch#fromFoldableOption]]
    */
  def empty[F[_]](implicit F: Applicative[F]): CommittableOffsetBatch[F] =
    new CommittableOffsetBatch[F] {
      override def updated(that: CommittableOffset[F]): CommittableOffsetBatch[F] =
        that.batch

      override def updated(that: CommittableOffsetBatch[F]): CommittableOffsetBatch[F] =
        that

      override val offsets: Map[TopicPartition, OffsetAndMetadata] =
        Map.empty

      override val commit: F[Unit] =
        F.unit

      override def toString: String =
        Show[CommittableOffsetBatch[F]].show(this)
    }

  implicit def committableOffsetBatchShow[F[_]]: Show[CommittableOffsetBatch[F]] =
    Show.show { cob =>
      if (cob.offsets.isEmpty) "CommittableOffsetBatch(<empty>)"
      else {
        val builder = new StringBuilder("CommittableOffsetBatch(")
        val offsets = cob.offsets.toList.sorted
        var first = true

        offsets.foreach {
          case (tp, oam) =>
            if (first) first = false
            else builder.append(", ")

            builder.append(tp.show).append(" -> ").append(oam.show)
        }

        builder.append(')').toString
      }
    }
}

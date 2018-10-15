package fs2.kafka

import cats.Applicative
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

sealed abstract class CommittableOffsetBatch[F[_]] {
  def updated(that: CommittableOffset[F]): CommittableOffsetBatch[F]

  def offsets: Map[TopicPartition, OffsetAndMetadata]

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
        CommittableOffsetBatch(this.offsets ++ that.offsets, _commit)

      override val offsets: Map[TopicPartition, OffsetAndMetadata] =
        _offsets

      override def commit: F[Unit] =
        _commit(offsets)
    }
  }

  def empty[F[_]](implicit F: Applicative[F]): CommittableOffsetBatch[F] =
    new CommittableOffsetBatch[F] {
      override def updated(that: CommittableOffset[F]): CommittableOffsetBatch[F] =
        that.batch

      override val offsets: Map[TopicPartition, OffsetAndMetadata] =
        Map.empty

      override val commit: F[Unit] =
        F.unit
    }
}

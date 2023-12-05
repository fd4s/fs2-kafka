package fs2.kafka.consumer

import cats.syntax.flatMap.*
import cats.MonadThrow
import fs2.*
import fs2.kafka.consumer.KafkaConsumeChunk.CommitNow
import fs2.kafka.CommittableConsumerRecord
import fs2.kafka.CommittableOffsetBatch
import fs2.kafka.ConsumerRecord

import org.apache.kafka.common.TopicPartition

trait KafkaConsumeChunk[F[_], K, V] extends KafkaConsume[F, K, V] {

  final def recordsChunk(
    processor: Chunk[ConsumerRecord[K, V]] => F[CommitNow.type]
  )(implicit F: MonadThrow[F]): Stream[F, Unit] = streamChunk(processor)

  final def streamChunk(
    processor: Chunk[ConsumerRecord[K, V]] => F[CommitNow.type]
  )(implicit F: MonadThrow[F]): Stream[F, Unit] =
    stream.chunks.evalMap(consumeChunk(processor))

  final def partitionedRecordsChunk(
    processor: Chunk[ConsumerRecord[K, V]] => F[CommitNow.type]
  )(implicit F: MonadThrow[F]): Stream[F, Stream[F, Unit]] =
    partitionedStreamChunk(processor)

  final def partitionedStreamChunk(
    processor: Chunk[ConsumerRecord[K, V]] => F[CommitNow.type]
  )(implicit F: MonadThrow[F]): Stream[F, Stream[F, Unit]] =
    partitionedStream.map(_.chunks.evalMap(consumeChunk(processor)))

  final def partitionsMapStreamChunk(
    processor: Chunk[ConsumerRecord[K, V]] => F[CommitNow.type]
  )(implicit F: MonadThrow[F]): Stream[F, Map[TopicPartition, Stream[F, Unit]]] =
    partitionsMapStream
      .map(m => m.map(kv => (kv._1, kv._2.chunks.evalMap(consumeChunk(processor)))))

  private def consumeChunk(processor: Chunk[ConsumerRecord[K, V]] => F[CommitNow.type])(
    chunk: Chunk[CommittableConsumerRecord[F, K, V]]
  )(implicit F: MonadThrow[F]): F[Unit] =
    processor(chunk.map(_.record)) >> CommittableOffsetBatch
      .fromFoldable(chunk.map(_.offset))
      .commit

}

object KafkaConsumeChunk {
  object CommitNow
}

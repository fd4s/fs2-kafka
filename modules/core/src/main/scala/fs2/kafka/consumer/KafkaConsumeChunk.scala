package fs2.kafka.consumer

import cats.syntax.flatMap.*
import fs2.*
import fs2.kafka.CommittableConsumerRecord
import fs2.kafka.CommittableOffsetBatch
import fs2.kafka.ConsumerRecord
import cats.MonadThrow

trait KafkaConsumeChunk[F[_], K, V] extends KafkaConsume[F, K, V] {

  final def streamChunk(
    processor: Chunk[ConsumerRecord[K, V]] => F[Unit]
  )(implicit F: MonadThrow[F]): Stream[F, Unit] =
    stream.chunks.evalMap(consumeChunk(processor))

  private def consumeChunk(processor: Chunk[ConsumerRecord[K, V]] => F[Unit])(
    chunk: Chunk[CommittableConsumerRecord[F, K, V]]
  )(implicit F: MonadThrow[F]): F[Unit] =
    processor(chunk.map(_.record)) >> CommittableOffsetBatch
      .fromFoldable(chunk.map(_.offset))
      .commit
}

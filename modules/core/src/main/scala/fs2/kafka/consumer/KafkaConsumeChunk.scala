package fs2.kafka.consumer

import cats.effect.Concurrent
import cats.syntax.flatMap.*
import cats.Monad
import fs2.*
import fs2.kafka.consumer.KafkaConsumeChunk.CommitNow
import fs2.kafka.CommittableConsumerRecord
import fs2.kafka.CommittableOffsetBatch
import fs2.kafka.ConsumerRecord

trait KafkaConsumeChunk[F[_], K, V] extends KafkaConsume[F, K, V] {

  final def consumeChunk(
    processor: Chunk[ConsumerRecord[K, V]] => F[CommitNow]
  )(implicit F: Concurrent[F]): F[Nothing] = partitionedStream
    .map(
      _.chunks.evalMap(consumeChunk(processor))
    )
    .parJoinUnbounded
    .compile
    .drain >> F.never

  private def consumeChunk(processor: Chunk[ConsumerRecord[K, V]] => F[CommitNow])(
    chunk: Chunk[CommittableConsumerRecord[F, K, V]]
  )(implicit F: Monad[F]): F[Unit] = {
    val (offsets, records) = chunk.foldLeft(
      (CommittableOffsetBatch.empty, Chunk.empty[ConsumerRecord[K, V]])
    )((acc, record) => (acc._1.updated(record.offset), acc._2 ++ Chunk(record.record)))

    processor(records) >> offsets.commit
  }

}

object KafkaConsumeChunk {

  type CommitNow = CommitNow.type
  object CommitNow

}

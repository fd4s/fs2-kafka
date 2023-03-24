/*
 * Copyright 2018-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.ApplicativeError
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.effect.Ref
import fs2.{Chunk, Stream}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

import scala.concurrent.duration._

final class KafkaSpec extends BaseAsyncSpec {
  describe("commitBatchWithin") {
    it("should batch commit according specified arguments") {
      val committed =
        (for {
          ref <- Stream.eval(Ref[IO].of(Option.empty[Map[TopicPartition, OffsetAndMetadata]]))
          commit = (offsets: Map[TopicPartition, OffsetAndMetadata]) => ref.set(Some(offsets))
          offsets = Chunk.seq(exampleOffsets(commit))
          _ <- Stream
            .chunk(offsets)
            .covary[IO]
            .through(commitBatchWithin(offsets.size, 10.seconds))
          result <- Stream.eval(ref.get)
        } yield result).compile.lastOrError.unsafeRunSync()

      assert(committed.contains(exampleOffsetsCommitted))
    }
  }

  def exampleOffsets[F[_]](
    commit: Map[TopicPartition, OffsetAndMetadata] => F[Unit]
  )(implicit F: ApplicativeError[F, Throwable]): List[CommittableOffset[F]] = List(
    CommittableOffset[F](
      new TopicPartition("topic", 0),
      new OffsetAndMetadata(1L),
      Some("group-1"),
      commit
    ),
    CommittableOffset[F](
      new TopicPartition("topic", 0),
      new OffsetAndMetadata(2L),
      Some("group-1"),
      commit
    ),
    CommittableOffset[F](
      new TopicPartition("topic", 1),
      new OffsetAndMetadata(1L),
      Some("group-1"),
      commit
    ),
    CommittableOffset[F](
      new TopicPartition("topic", 1),
      new OffsetAndMetadata(2L),
      Some("group-1"),
      commit
    ),
    CommittableOffset[F](
      new TopicPartition("topic", 1),
      new OffsetAndMetadata(3L),
      Some("group-1"),
      commit
    )
  )

  val exampleOffsetsCommitted: Map[TopicPartition, OffsetAndMetadata] = Map(
    new TopicPartition("topic", 0) -> new OffsetAndMetadata(2L),
    new TopicPartition("topic", 1) -> new OffsetAndMetadata(3L)
  )
}

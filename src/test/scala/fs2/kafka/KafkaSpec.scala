package fs2.kafka

import cats.effect.IO
import cats.effect.concurrent.Ref
import fs2.{Chunk, Stream}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

import scala.concurrent.duration._

final class KafkaSpec extends BaseAsyncSpec {
  describe("commitBatch") {
    it("should batch commit each chunk") {
      val committed =
        (for {
          ref <- Stream.eval(Ref[IO].of(Option.empty[Map[TopicPartition, OffsetAndMetadata]]))
          commit = (offsets: Map[TopicPartition, OffsetAndMetadata]) => ref.set(Some(offsets))
          offsets = Chunk.seq(exampleOffsets(commit))
          _ <- Stream
            .chunk(offsets)
            .covary[IO]
            .to(commitBatch)
          result <- Stream.eval(ref.get)
        } yield result).compile.lastOrError.unsafeRunSync

      assert(committed.contains(exampleOffsetsCommitted))
    }
  }

  describe("commitBatchF") {
    it("should batch commit each chunk") {
      val committed =
        (for {
          ref <- Stream.eval(Ref[IO].of(Option.empty[Map[TopicPartition, OffsetAndMetadata]]))
          commit = (offsets: Map[TopicPartition, OffsetAndMetadata]) => ref.set(Some(offsets))
          offsets = Chunk.seq(exampleOffsets(commit))
          _ <- Stream
            .chunk(offsets)
            .covary[IO]
            .map(IO.pure)
            .to(commitBatchF)
          result <- Stream.eval(ref.get)
        } yield result).compile.lastOrError.unsafeRunSync

      assert(committed.contains(exampleOffsetsCommitted))
    }
  }

  describe("commitBatchOption") {
    it("should batch commit each chunk") {
      val committed =
        (for {
          ref <- Stream.eval(Ref[IO].of(Option.empty[Map[TopicPartition, OffsetAndMetadata]]))
          commit = (offsets: Map[TopicPartition, OffsetAndMetadata]) => ref.set(Some(offsets))
          offsets = Chunk.seq(exampleOffsetsOption(commit))
          _ <- Stream
            .chunk(offsets)
            .covary[IO]
            .to(commitBatchOption)
          result <- Stream.eval(ref.get)
        } yield result).compile.lastOrError.unsafeRunSync

      assert(committed.contains(exampleOffsetsCommitted))
    }
  }

  describe("commitBatchOptionF") {
    it("should batch commit each chunk") {
      val committed =
        (for {
          ref <- Stream.eval(Ref[IO].of(Option.empty[Map[TopicPartition, OffsetAndMetadata]]))
          commit = (offsets: Map[TopicPartition, OffsetAndMetadata]) => ref.set(Some(offsets))
          offsets = Chunk.seq(exampleOffsetsOption(commit))
          _ <- Stream
            .chunk(offsets)
            .covary[IO]
            .map(IO.pure)
            .to(commitBatchOptionF)
          result <- Stream.eval(ref.get)
        } yield result).compile.lastOrError.unsafeRunSync

      assert(committed.contains(exampleOffsetsCommitted))
    }
  }

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
            .to(commitBatchWithin(offsets.size, 10.seconds))
          result <- Stream.eval(ref.get)
        } yield result).compile.lastOrError.unsafeRunSync

      assert(committed.contains(exampleOffsetsCommitted))
    }
  }

  describe("commitBatchWithinF") {
    it("should batch commit according specified arguments") {
      val committed =
        (for {
          ref <- Stream.eval(Ref[IO].of(Option.empty[Map[TopicPartition, OffsetAndMetadata]]))
          commit = (offsets: Map[TopicPartition, OffsetAndMetadata]) => ref.set(Some(offsets))
          offsets = Chunk.seq(exampleOffsets(commit))
          _ <- Stream
            .chunk(offsets)
            .covary[IO]
            .map(IO.pure)
            .to(commitBatchWithinF(offsets.size, 10.seconds))
          result <- Stream.eval(ref.get)
        } yield result).compile.lastOrError.unsafeRunSync

      assert(committed.contains(exampleOffsetsCommitted))
    }
  }

  describe("commitBatchOptionWithin") {
    it("should batch commit according specified arguments") {
      val committed =
        (for {
          ref <- Stream.eval(Ref[IO].of(Option.empty[Map[TopicPartition, OffsetAndMetadata]]))
          commit = (offsets: Map[TopicPartition, OffsetAndMetadata]) => ref.set(Some(offsets))
          offsets = Chunk.seq(exampleOffsetsOption(commit))
          _ <- Stream
            .chunk(offsets)
            .covary[IO]
            .to(commitBatchOptionWithin(offsets.size, 10.seconds))
          result <- Stream.eval(ref.get)
        } yield result).compile.lastOrError.unsafeRunSync

      assert(committed.contains(exampleOffsetsCommitted))
    }
  }

  describe("commitBatchOptionWithinF") {
    it("should batch commit according specified arguments") {
      val committed =
        (for {
          ref <- Stream.eval(Ref[IO].of(Option.empty[Map[TopicPartition, OffsetAndMetadata]]))
          commit = (offsets: Map[TopicPartition, OffsetAndMetadata]) => ref.set(Some(offsets))
          offsets = Chunk.seq(exampleOffsetsOption(commit))
          _ <- Stream
            .chunk(offsets)
            .covary[IO]
            .map(IO.pure)
            .to(commitBatchOptionWithinF(offsets.size, 10.seconds))
          result <- Stream.eval(ref.get)
        } yield result).compile.lastOrError.unsafeRunSync

      assert(committed.contains(exampleOffsetsCommitted))
    }
  }

  def exampleOffsetsOption[F[_]](
    commit: Map[TopicPartition, OffsetAndMetadata] => F[Unit]
  ): List[Option[CommittableOffset[F]]] = {
    val (first, rest) = exampleOffsets(commit).map(Some(_)).splitAt(2)
    List(None) ++ first ++ List(None) ++ rest ++ List(None)
  }

  def exampleOffsets[F[_]](
    commit: Map[TopicPartition, OffsetAndMetadata] => F[Unit]
  ): List[CommittableOffset[F]] = List(
    CommittableOffset[F](new TopicPartition("topic", 0), new OffsetAndMetadata(1L), commit),
    CommittableOffset[F](new TopicPartition("topic", 0), new OffsetAndMetadata(2L), commit),
    CommittableOffset[F](new TopicPartition("topic", 1), new OffsetAndMetadata(1L), commit),
    CommittableOffset[F](new TopicPartition("topic", 1), new OffsetAndMetadata(2L), commit),
    CommittableOffset[F](new TopicPartition("topic", 1), new OffsetAndMetadata(3L), commit)
  )

  val exampleOffsetsCommitted: Map[TopicPartition, OffsetAndMetadata] = Map(
    new TopicPartition("topic", 0) -> new OffsetAndMetadata(2L),
    new TopicPartition("topic", 1) -> new OffsetAndMetadata(3L)
  )
}

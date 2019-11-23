package fs2.kafka

import cats.ApplicativeError
import cats.effect.IO
import cats.effect.concurrent.Ref
import fs2.{Chunk, Stream}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

import scala.concurrent.duration._

final class KafkaSpec extends BaseAsyncSpec {
  describe("kafkaProducer") {
    it("should be able to create a mock kafka producer ") {
      val p1 = ProducerRecord("topic1", "1", "value1")
      val p2 = ProducerRecord("topic1", "2", "value2")
      val p3 = ProducerRecord("topic1", "3", "value3")
      val p4 = ProducerRecord("topic2", "1", "value1")
      val p5 = ProducerRecord("topic2", "2", "value3")
      val p6 = ProducerRecord("topic2", "3", "value3")
      val p7 = ProducerRecord("topic3", "1", "value1")
      val chunks: Chunk[ProducerRecord[String, String]] = Chunk(p1, p2, p3, p4, p5, p6, p7)
      val producerRecords: ProducerRecords[String, String, Int] = ProducerRecords(chunks, 2)
      val kafkaProducer: IO[KafkaProducer[IO, String, String]] =
        KafkaProducer.unit[IO, String, String]
      val producerResult: ProducerResult[String, String, Int] =
        kafkaProducer.flatMap(_.produce(producerRecords)).unsafeRunSync().unsafeRunSync()
      producerResult.records.size shouldBe 7
      val list = producerResult.records.toList
      list(0)._1 shouldBe p1
      list(0)._2.offset() shouldBe 1L
      list(1)._1 shouldBe p2
      list(1)._2.offset() shouldBe 2L
      list(2)._1 shouldBe p3
      list(2)._2.offset() shouldBe 3L
      list(3)._1 shouldBe p4
      list(3)._2.offset() shouldBe 1L
      list(4)._1 shouldBe p5
      list(4)._2.offset() shouldBe 2L
      list(5)._1 shouldBe p6
      list(5)._2.offset() shouldBe 3L
      list(6)._1 shouldBe p7
      list(6)._2.offset() shouldBe 1L
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
            .through(commitBatchWithin(offsets.size, 10.seconds))
          result <- Stream.eval(ref.get)
        } yield result).compile.lastOrError.unsafeRunSync

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

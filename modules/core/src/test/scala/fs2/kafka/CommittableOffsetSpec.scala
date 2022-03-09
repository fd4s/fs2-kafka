package fs2.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata

final class CommittableOffsetSpec extends BaseSpec {
  describe("CommittableOffset") {
    it("should be able to commit the offset") {
      val partition = new TopicPartition("topic", 0)
      val offsetAndMetadata = new OffsetAndMetadata(0L, "metadata")
      var committed: Map[TopicPartition, OffsetAndMetadata] = null

      CommittableOffset[IO](
        partition,
        offsetAndMetadata,
        groupMetadata = None,
        commit = offsets => IO { committed = offsets }
      ).commit.unsafeRunSync()

      assert(committed == Map(partition -> offsetAndMetadata))
    }

    it("should have a Show instance and matching toString") {
      val partition = new TopicPartition("topic", 0)

      assert {
        val offsetAndMetadata = new OffsetAndMetadata(0L, "metadata")
        val offset = CommittableOffset[IO](partition, offsetAndMetadata, None, _ => IO.unit)

        offset.toString == "CommittableOffset(topic-0 -> (0, metadata))" &&
        offset.show == offset.toString
      }

      assert {
        val offsetAndMetadata = new OffsetAndMetadata(0L, "metadata")
        val offset =
          CommittableOffset[IO](
            partition,
            offsetAndMetadata,
            Some(new ConsumerGroupMetadata("the-group")),
            _ => IO.unit
          )

        offset.toString == "CommittableOffset(topic-0 -> (0, metadata), the-group)" &&
        offset.show == offset.toString
      }

      assert {
        val offsetAndMetadata = new OffsetAndMetadata(0L)
        val offset = CommittableOffset[IO](partition, offsetAndMetadata, None, _ => IO.unit)

        offset.toString == "CommittableOffset(topic-0 -> 0)" &&
        offset.show == offset.toString
      }

      assert {
        val offsetAndMetadata = new OffsetAndMetadata(0L)
        val offset =
          CommittableOffset[IO](
            partition,
            offsetAndMetadata,
            Some(new ConsumerGroupMetadata("the-group")),
            _ => IO.unit
          )

        offset.toString == "CommittableOffset(topic-0 -> 0, the-group)" &&
        offset.show == offset.toString
      }
    }
  }

}

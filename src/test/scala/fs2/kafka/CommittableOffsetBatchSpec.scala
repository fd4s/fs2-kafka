package fs2.kafka

import cats.implicits._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.scalacheck.Gen

final class CommittableOffsetBatchSpec extends BaseSpec {
  describe("CommittableOffsetBatch#empty") {
    val empty: CommittableOffsetBatch[Id] =
      CommittableOffsetBatch.empty

    it("should have no offsets") {
      assert(empty.offsets.isEmpty)
    }

    it("should return updated offset as batch") {
      forAll { offset: CommittableOffset[Id] =>
        val updated = empty.updated(offset)
        assert(updated.offsets == offset.offsets)
      }
    }
  }

  describe("CommittableOffsetBatch#updated") {
    it("should include the provided offset") {
      forAll { (batch: CommittableOffsetBatch[Id], offset: CommittableOffset[Id]) =>
        val updatedOffset = batch.updated(offset).offsets.get(offset.topicPartition)
        assert(updatedOffset.contains(offset.offsetAndMetadata))
      }
    }

    it("should override using the provided offset") {
      forAll { batch: CommittableOffsetBatch[Id] =>
        whenever(batch.offsets.nonEmpty) {
          Gen.oneOf(batch.offsets.toList).sample.foreach { offset =>
            val (topicPartition, offsetAndMetadata) = offset

            val newOffsetAndMetadata =
              new OffsetAndMetadata(Long.MaxValue, offsetAndMetadata.metadata)

            val committable =
              CommittableOffset[Id](
                topicPartition = topicPartition,
                offsetAndMetadata = newOffsetAndMetadata,
                consumerGroupId = None,
                commit = _ => ()
              )

            val updatedOffset = batch.updated(committable).offsets.get(topicPartition)
            assert(updatedOffset.contains(newOffsetAndMetadata))
          }
        }
      }
    }

    it("should be able to update with batch") {
      forAll { (batch1: CommittableOffsetBatch[Id], batch2: CommittableOffsetBatch[Id]) =>
        val result = batch1.updated(batch2)

        val offsets = batch2.offsets.map {
          case (topicPartition, offsetAndMetadata) =>
            CommittableOffset[Id](topicPartition, offsetAndMetadata, None, _ => ())
        }

        val expected = offsets.foldLeft(batch1)(_ updated _)

        assert(result.offsets == expected.offsets)
      }
    }
  }

  describe("CommittableOffsetBatch#toString") {
    it("should provide a Show instance and matching toString") {
      assert {
        CommittableOffsetBatch.empty[Id].toString == "CommittableOffsetBatch(<empty>)" &&
        CommittableOffsetBatch.empty[Id].show == CommittableOffsetBatch.empty[Id].show
      }

      val one = CommittableOffset[Id](
        new TopicPartition("topic", 0),
        new OffsetAndMetadata(0L),
        None,
        _ => ()
      ).batch

      assert {
        one.toString == "CommittableOffsetBatch(topic-0 -> 0)" &&
        one.show == one.toString
      }

      val oneMetadata = CommittableOffset[Id](
        new TopicPartition("topic", 1),
        new OffsetAndMetadata(0L, "metadata"),
        None,
        _ => ()
      ).batch

      assert {
        oneMetadata.toString == "CommittableOffsetBatch(topic-1 -> (0, metadata))" &&
        oneMetadata.show == oneMetadata.toString
      }

      val two = one updated oneMetadata

      assert {
        two.toString == "CommittableOffsetBatch(topic-0 -> 0, topic-1 -> (0, metadata))" &&
        two.show == two.toString
      }
    }
  }
}

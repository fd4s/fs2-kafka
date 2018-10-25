package fs2.kafka

import cats.Id
import org.apache.kafka.clients.consumer.OffsetAndMetadata
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
                commit = _ => ()
              )

            val updatedOffset = batch.updated(committable).offsets.get(topicPartition)
            assert(updatedOffset.contains(newOffsetAndMetadata))
          }
        }
      }
    }
  }
}

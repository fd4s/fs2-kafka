package fs2.kafka

import cats.data.NonEmptyList
import cats.effect.IO
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

class TransactionalProducerMessageSpec extends BaseSpec {
  describe("TransactionalProducerMessageSpec") {
    it("should be able to create with one record") {
      val record = ProducerRecord("topic", "key", "value")

      val offset =
        CommittableOffset[IO](
          new TopicPartition("topic", 1),
          new OffsetAndMetadata(1),
          Some("the-group"),
          _ => IO.unit
        )

      assert {
        TransactionalProducerMessage
          .one(CommittableProducerRecords.one(record, offset), 123)
          .toString == "TransactionalProducerMessage(CommittableProducerRecords(ProducerRecord(topic = topic, key = key, value = value), CommittableOffset(topic-1 -> 1, the-group)), 123)" &&
        TransactionalProducerMessage
          .one(CommittableProducerRecords.one(record, offset))
          .toString == "TransactionalProducerMessage(CommittableProducerRecords(ProducerRecord(topic = topic, key = key, value = value), CommittableOffset(topic-1 -> 1, the-group)), ())"
      }
    }

    it("should be able to create with multiple records") {
      val records = NonEmptyList.of(
        ProducerRecord("topic", "key", "value"),
        ProducerRecord("topic2", "key2", "value2")
      )

      val offset = CommittableOffset[IO](
        new TopicPartition("topic", 1),
        new OffsetAndMetadata(1),
        Some("the-group"),
        _ => IO.unit
      )

      assert {
        TransactionalProducerMessage
          .one(CommittableProducerRecords(records, offset), 123)
          .toString == "TransactionalProducerMessage(NonEmptyList(CommittableProducerRecords(NonEmptyList(ProducerRecord(topic = topic, key = key, value = value), ProducerRecord(topic = topic2, key = key2, value = value2)), CommittableOffset(topic-1 -> 1, the-group))), 123)" &&
        TransactionalProducerMessage
          .one(CommittableProducerRecords(records, offset))
          .toString == "TransactionalProducerMessage(NonEmptyList(CommittableProducerRecords(NonEmptyList(ProducerRecord(topic = topic, key = key, value = value), ProducerRecord(topic = topic2, key = key2, value = value2)), CommittableOffset(topic-1 -> 1, the-group))), ())"
      }
    }
  }
}

package fs2.kafka

import cats.data.NonEmptyList
import cats.effect.IO
import cats.implicits._
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
          .one(record, offset.batch, 123)
          .unsafeRunSync()
          .toString == "TransactionalProducerMessage(ProducerRecord(topic = topic, key = key, value = value), [topic-1 -> 1], the-group, 123)" &&
        TransactionalProducerMessage
          .one(record, offset.batch, 123)
          .unsafeRunSync()
          .show == "TransactionalProducerMessage(ProducerRecord(topic = topic, key = key, value = value), [topic-1 -> 1], the-group, 123)" &&
        TransactionalProducerMessage
          .one(record, offset.batch)
          .unsafeRunSync()
          .toString == "TransactionalProducerMessage(ProducerRecord(topic = topic, key = key, value = value), [topic-1 -> 1], the-group, ())" &&
        TransactionalProducerMessage
          .one(record, offset.batch)
          .unsafeRunSync()
          .show == "TransactionalProducerMessage(ProducerRecord(topic = topic, key = key, value = value), [topic-1 -> 1], the-group, ())"
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
        TransactionalProducerMessage(records, offset.batch, 123)
          .unsafeRunSync()
          .toString == "TransactionalProducerMessage(ProducerRecord(topic = topic, key = key, value = value), ProducerRecord(topic = topic2, key = key2, value = value2), [topic-1 -> 1], the-group, 123)" &&
        TransactionalProducerMessage(records, offset.batch, 123)
          .unsafeRunSync()
          .show == "TransactionalProducerMessage(ProducerRecord(topic = topic, key = key, value = value), ProducerRecord(topic = topic2, key = key2, value = value2), [topic-1 -> 1], the-group, 123)" &&
        TransactionalProducerMessage(records, offset.batch)
          .unsafeRunSync()
          .toString == "TransactionalProducerMessage(ProducerRecord(topic = topic, key = key, value = value), ProducerRecord(topic = topic2, key = key2, value = value2), [topic-1 -> 1], the-group, ())" &&
        TransactionalProducerMessage(records, offset.batch)
          .unsafeRunSync()
          .show == "TransactionalProducerMessage(ProducerRecord(topic = topic, key = key, value = value), ProducerRecord(topic = topic2, key = key2, value = value2), [topic-1 -> 1], the-group, ())" &&
        TransactionalProducerMessage[NonEmptyList]
          .of(records, offset.batch, 123)
          .unsafeRunSync()
          .toString == "TransactionalProducerMessage(ProducerRecord(topic = topic, key = key, value = value), ProducerRecord(topic = topic2, key = key2, value = value2), [topic-1 -> 1], the-group, 123)" &&
        TransactionalProducerMessage[NonEmptyList]
          .of(records, offset.batch, 123)
          .unsafeRunSync()
          .show == "TransactionalProducerMessage(ProducerRecord(topic = topic, key = key, value = value), ProducerRecord(topic = topic2, key = key2, value = value2), [topic-1 -> 1], the-group, 123)" &&
        TransactionalProducerMessage[NonEmptyList]
          .of(records, offset.batch)
          .unsafeRunSync()
          .toString == "TransactionalProducerMessage(ProducerRecord(topic = topic, key = key, value = value), ProducerRecord(topic = topic2, key = key2, value = value2), [topic-1 -> 1], the-group, ())" &&
        TransactionalProducerMessage[NonEmptyList]
          .of(records, offset.batch)
          .unsafeRunSync()
          .show == "TransactionalProducerMessage(ProducerRecord(topic = topic, key = key, value = value), ProducerRecord(topic = topic2, key = key2, value = value2), [topic-1 -> 1], the-group, ())"
      }
    }

    it("should be able to create with multiple offsets from the same consumer group") {
      val record = ProducerRecord("topic", "key", "value")
      val offsets = NonEmptyList
        .of(1, 2)
        .map { i =>
          CommittableOffset[IO](
            new TopicPartition("topic", i),
            new OffsetAndMetadata(1),
            Some("the-group"),
            _ => IO.unit
          )
        }
      val batch = CommittableOffsetBatch.fromFoldable(offsets)

      assert {
        TransactionalProducerMessage
          .one(record, batch, 123)
          .unsafeRunSync()
          .toString == "TransactionalProducerMessage(ProducerRecord(topic = topic, key = key, value = value), [topic-1 -> 1, topic-2 -> 1], the-group, 123)" &&
          TransactionalProducerMessage
            .one(record, batch, 123)
            .unsafeRunSync()
            .show == "TransactionalProducerMessage(ProducerRecord(topic = topic, key = key, value = value), [topic-1 -> 1, topic-2 -> 1], the-group, 123)" &&
          TransactionalProducerMessage
            .one(record, batch)
            .unsafeRunSync()
            .toString == "TransactionalProducerMessage(ProducerRecord(topic = topic, key = key, value = value), [topic-1 -> 1, topic-2 -> 1], the-group, ())" &&
          TransactionalProducerMessage
            .one(record, batch)
            .unsafeRunSync()
            .show == "TransactionalProducerMessage(ProducerRecord(topic = topic, key = key, value = value), [topic-1 -> 1, topic-2 -> 1], the-group, ())"
      }
    }

    it("should fail to create with multiple consumer group IDs") {
      val records = NonEmptyList.of(1, 2).map(i => ProducerRecord("topic", "key", s"value-$i"))
      val batch = CommittableOffsetBatch.fromFoldable {
        NonEmptyList
          .of(1, 2)
          .map { i =>
            CommittableOffset[IO](
              new TopicPartition("topic", 1),
              new OffsetAndMetadata(1),
              Some(s"the-group-$i"),
              _ => IO.unit
            )
          }
      }

      TransactionalProducerMessage(records, batch).attempt.unsafeRunSync().isLeft shouldBe true
    }

    it("should fail to create with no consumer group IDs") {
      val record = ProducerRecord("topic", "key", "value")

      TransactionalProducerMessage
        .one(record, CommittableOffsetBatch.empty[IO])
        .attempt
        .unsafeRunSync()
        .isLeft shouldBe true
    }
  }
}

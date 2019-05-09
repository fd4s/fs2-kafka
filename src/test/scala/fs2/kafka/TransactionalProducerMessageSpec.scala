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
          .one(record -> offset, 123)
          .unsafeRunSync()
          .toString == "TransactionalProducerMessage(ProducerRecord(topic = topic, key = key, value = value), CommittableOffsetBatch(topic-1 -> 1), the-group, 123)" &&
        TransactionalProducerMessage
          .one(record -> offset, 123)
          .unsafeRunSync()
          .show == "TransactionalProducerMessage(ProducerRecord(topic = topic, key = key, value = value), CommittableOffsetBatch(topic-1 -> 1), the-group, 123)" &&
        TransactionalProducerMessage
          .one(record -> offset)
          .unsafeRunSync()
          .toString == "TransactionalProducerMessage(ProducerRecord(topic = topic, key = key, value = value), CommittableOffsetBatch(topic-1 -> 1), the-group, ())" &&
        TransactionalProducerMessage
          .one(record -> offset)
          .unsafeRunSync()
          .show == "TransactionalProducerMessage(ProducerRecord(topic = topic, key = key, value = value), CommittableOffsetBatch(topic-1 -> 1), the-group, ())"
      }
    }

    it("should be able to create with multiple records") {
      val records = NonEmptyList.of(ProducerRecord("topic", "key", "value"))
      val offsets = NonEmptyList.of(
        CommittableOffset[IO](
          new TopicPartition("topic", 1),
          new OffsetAndMetadata(1),
          Some("the-group"),
          _ => IO.unit
        )
      )
      val zipped = records.zipWith(offsets)(Tuple2.apply)

      assert {
        TransactionalProducerMessage(zipped, 123)
          .unsafeRunSync()
          .toString == "TransactionalProducerMessage(ProducerRecord(topic = topic, key = key, value = value), CommittableOffsetBatch(topic-1 -> 1), the-group, 123)" &&
        TransactionalProducerMessage(zipped, 123)
          .unsafeRunSync()
          .show == "TransactionalProducerMessage(ProducerRecord(topic = topic, key = key, value = value), CommittableOffsetBatch(topic-1 -> 1), the-group, 123)" &&
        TransactionalProducerMessage(zipped)
          .unsafeRunSync()
          .toString == "TransactionalProducerMessage(ProducerRecord(topic = topic, key = key, value = value), CommittableOffsetBatch(topic-1 -> 1), the-group, ())" &&
        TransactionalProducerMessage(zipped)
          .unsafeRunSync()
          .show == "TransactionalProducerMessage(ProducerRecord(topic = topic, key = key, value = value), CommittableOffsetBatch(topic-1 -> 1), the-group, ())" &&
        TransactionalProducerMessage[NonEmptyList]
          .of(zipped, 123)
          .unsafeRunSync()
          .toString == "TransactionalProducerMessage(ProducerRecord(topic = topic, key = key, value = value), CommittableOffsetBatch(topic-1 -> 1), the-group, 123)" &&
        TransactionalProducerMessage[NonEmptyList]
          .of(zipped, 123)
          .unsafeRunSync()
          .show == "TransactionalProducerMessage(ProducerRecord(topic = topic, key = key, value = value), CommittableOffsetBatch(topic-1 -> 1), the-group, 123)" &&
        TransactionalProducerMessage[NonEmptyList]
          .of(zipped)
          .unsafeRunSync()
          .toString == "TransactionalProducerMessage(ProducerRecord(topic = topic, key = key, value = value), CommittableOffsetBatch(topic-1 -> 1), the-group, ())" &&
        TransactionalProducerMessage[NonEmptyList]
          .of(zipped)
          .unsafeRunSync()
          .show == "TransactionalProducerMessage(ProducerRecord(topic = topic, key = key, value = value), CommittableOffsetBatch(topic-1 -> 1), the-group, ())"
      }
    }

    it("should fail to create with multiple consumer group IDs") {
      val records = NonEmptyList.of(1, 2).map(i => ProducerRecord("topic", "key", s"value-$i"))
      val offsets = NonEmptyList
        .of(1, 2)
        .map(
          i =>
            CommittableOffset[IO](
              new TopicPartition("topic", 1),
              new OffsetAndMetadata(1),
              Some(s"the-group-$i"),
              _ => IO.unit
          )
        )
      val zipped = records.zipWith(offsets)(Tuple2.apply)

      TransactionalProducerMessage(zipped).attempt.unsafeRunSync().isLeft shouldBe true
    }

    it("should fail to create with no consumer group IDs") {
      val record = ProducerRecord("topic", "key", "value")
      val offset =
        CommittableOffset[IO](
          new TopicPartition("topic", 1),
          new OffsetAndMetadata(1),
          None,
          _ => IO.unit
        )

      TransactionalProducerMessage
        .one(record -> offset)
        .attempt
        .unsafeRunSync()
        .isLeft shouldBe true
    }
  }
}

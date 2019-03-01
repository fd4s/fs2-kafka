package fs2.kafka

import cats.implicits._
import org.apache.kafka.clients.consumer.ConsumerRecord.{NULL_SIZE, NO_TIMESTAMP}
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.record.TimestampType._
import org.scalatest._

final class ConsumerRecordSpec extends BaseSpec {
  describe("ConsumerRecord#fromJava") {
    it("should convert timestamps") {
      def check(timestamp: Long, timestampType: TimestampType)(
        f: ConsumerRecord[String, String] => Assertion
      ): Assertion = {
        val record =
          new KafkaConsumerRecord("topic", 0, 1, timestamp, timestampType, 2, 3, 4, "key", "value")
        f(ConsumerRecord.fromJava(record))
      }

      check(NO_TIMESTAMP, NO_TIMESTAMP_TYPE)(_.timestamp.isEmpty shouldBe true)
      check(NO_TIMESTAMP, CREATE_TIME)(_.timestamp.isEmpty shouldBe true)
      check(NO_TIMESTAMP, LOG_APPEND_TIME)(_.timestamp.isEmpty shouldBe true)

      check(0, NO_TIMESTAMP_TYPE)(_.timestamp.isEmpty shouldBe true)
      check(0, CREATE_TIME)(_.timestamp.createTime shouldBe Some(0))
      check(0, LOG_APPEND_TIME)(_.timestamp.logAppendTime shouldBe Some(0))
    }

    it("should convert serialized key size") {
      def check(serializedKeySize: Int)(
        f: ConsumerRecord[String, String] => Assertion
      ): Assertion = {
        val record =
          new KafkaConsumerRecord(
            "topic",
            0,
            1,
            NO_TIMESTAMP,
            NO_TIMESTAMP_TYPE,
            2,
            serializedKeySize,
            4,
            "key",
            "value"
          )

        f(ConsumerRecord.fromJava(record))
      }

      check(NULL_SIZE)(_.serializedKeySize shouldBe None)
      check(0)(_.serializedKeySize shouldBe Some(0))
    }

    it("should convert serialized value size") {
      def check(serializedValueSize: Int)(
        f: ConsumerRecord[String, String] => Assertion
      ): Assertion = {
        val record =
          new KafkaConsumerRecord(
            "topic",
            0,
            1,
            NO_TIMESTAMP,
            NO_TIMESTAMP_TYPE,
            2,
            3,
            serializedValueSize,
            "key",
            "value"
          )

        f(ConsumerRecord.fromJava(record))
      }

      check(NULL_SIZE)(_.serializedValueSize shouldBe None)
      check(0)(_.serializedValueSize shouldBe Some(0))
    }

    it("should convert leader epoch") {
      def check(leaderEpoch: Option[Int])(
        f: ConsumerRecord[String, String] => Assertion
      ): Assertion = {
        val record =
          new KafkaConsumerRecord(
            "topic",
            0,
            1,
            NO_TIMESTAMP,
            NO_TIMESTAMP_TYPE,
            2,
            3,
            4,
            "key",
            "value",
            Headers.empty.asJava,
            if (leaderEpoch.nonEmpty)
              java.util.Optional.of[java.lang.Integer](leaderEpoch.get)
            else java.util.Optional.empty()
          )

        f(ConsumerRecord.fromJava(record))
      }

      check(None)(_.leaderEpoch shouldBe None)
      check(Some(1))(_.leaderEpoch shouldBe Some(1))
    }
  }

  describe("ConsumerRecord#toString") {
    it("should include headers when present") {
      val record =
        ConsumerRecord("topic", 0, 1, "key", "value")
          .withHeaders(Header("key", Array[Byte]()).headers)

      val expected =
        "ConsumerRecord(topic = topic, partition = 0, offset = 1, key = key, value = value, headers = Headers(key -> []))"

      record.toString shouldBe expected
      record.show shouldBe expected
    }

    it("should include timestamp if present") {
      val record =
        ConsumerRecord("topic", 0, 1, "key", "value")
          .withTimestamp(Timestamp.createTime(0))

      val expected =
        "ConsumerRecord(topic = topic, partition = 0, offset = 1, key = key, value = value, timestamp = Timestamp(createTime = 0))"

      record.toString shouldBe expected
      record.show shouldBe expected
    }

    it("should include serialized key size if present") {
      val record =
        ConsumerRecord("topic", 0, 1, "key", "value")
          .withSerializedKeySize(1)

      val expected =
        "ConsumerRecord(topic = topic, partition = 0, offset = 1, key = key, value = value, serializedKeySize = 1)"

      record.toString shouldBe expected
      record.show shouldBe expected
    }

    it("should include serialized value size if present") {
      val record =
        ConsumerRecord("topic", 0, 1, "key", "value")
          .withSerializedValueSize(1)

      val expected =
        "ConsumerRecord(topic = topic, partition = 0, offset = 1, key = key, value = value, serializedValueSize = 1)"

      record.toString shouldBe expected
      record.show shouldBe expected
    }

    it("should include leader epoch if present") {
      val record =
        ConsumerRecord("topic", 0, 1, "key", "value")
          .withLeaderEpoch(1)

      val expected =
        "ConsumerRecord(topic = topic, partition = 0, offset = 1, key = key, value = value, leaderEpoch = 1)"

      record.toString shouldBe expected
      record.show shouldBe expected
    }
  }
}

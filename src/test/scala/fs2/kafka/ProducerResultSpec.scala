package fs2.kafka

import cats.implicits._
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition

final class ProducerResultSpec extends BaseSpec {
  describe("ProducerResult") {
    it("should have a Show instance and matching toString") {
      val empty: List[(ProducerRecord[String, String], RecordMetadata)] = Nil

      assert {
        ProducerResult(empty, 123).toString == "ProducerResult(<empty>, 123)" &&
        ProducerResult(empty, 123).show == ProducerResult(empty, 123).toString
      }

      val one: List[(ProducerRecord[String, String], RecordMetadata)] =
        List(
          new ProducerRecord("topic", 0, 0L, "key", "value") ->
            new RecordMetadata(new TopicPartition("topic", 0), 0L, 0L, 0L, 0L, 0, 0)
        )

      assert {
        ProducerResult(one, 123).toString == "ProducerResult(topic-0@0 -> ProducerRecord(topic=topic, partition=0, headers=RecordHeaders(headers = [], isReadOnly = false), key=key, value=value, timestamp=0), 123)" &&
        ProducerResult(one, 123).show == "ProducerResult(topic-0@0 -> ProducerRecord(topic = topic, partition = 0, headers = Headers(<empty>), key = key, value = value, timestamp = 0), 123)"
      }

      val two: List[(ProducerRecord[String, String], RecordMetadata)] =
        List(
          new ProducerRecord("topic", 0, 0L, "key", "value") ->
            new RecordMetadata(new TopicPartition("topic", 0), 0L, 0L, 0L, 0L, 0, 0),
          new ProducerRecord("topic", 1, 0L, "key", "value") ->
            new RecordMetadata(new TopicPartition("topic", 1), 0L, 0L, 0L, 0L, 0, 0)
        )

      assert {
        ProducerResult(two, 123).toString == "ProducerResult(topic-0@0 -> ProducerRecord(topic=topic, partition=0, headers=RecordHeaders(headers = [], isReadOnly = false), key=key, value=value, timestamp=0), topic-1@0 -> ProducerRecord(topic=topic, partition=1, headers=RecordHeaders(headers = [], isReadOnly = false), key=key, value=value, timestamp=0), 123)" &&
        ProducerResult(two, 123).show == "ProducerResult(topic-0@0 -> ProducerRecord(topic = topic, partition = 0, headers = Headers(<empty>), key = key, value = value, timestamp = 0), topic-1@0 -> ProducerRecord(topic = topic, partition = 1, headers = Headers(<empty>), key = key, value = value, timestamp = 0), 123)"
      }
    }
  }
}

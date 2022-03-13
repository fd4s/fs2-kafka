package fs2.kafka

import fs2.Chunk
import cats.syntax.all._
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition

final class ProducerResultSpec extends BaseSpec {
  describe("ProducerResult") {
    it("should have a Show instance and matching toString") {
      val empty: Chunk[(ProducerRecord[String, String], RecordMetadata)] = Chunk.empty

      assert {
        ProducerResult(empty).toString == "ProducerResult(<empty>)" &&
        ProducerResult(empty).show == ProducerResult(empty).toString
      }

      val one: Chunk[(ProducerRecord[String, String], RecordMetadata)] =
        Chunk.singleton(
          ProducerRecord("topic", "key", "value")
            .withPartition(1)
            .withTimestamp(0L)
            .withHeaders(Headers(Header("key", Array[Byte]()))) ->
            new RecordMetadata(new TopicPartition("topic", 0), 0L, 0, 0L, 0, 0)
        )

      assert {
        ProducerResult(one).toString == "ProducerResult(topic-0@0 -> ProducerRecord(topic = topic, partition = 1, timestamp = 0, key = key, value = value, headers = Headers(key -> [])))" &&
        ProducerResult(one).show == "ProducerResult(topic-0@0 -> ProducerRecord(topic = topic, partition = 1, timestamp = 0, key = key, value = value, headers = Headers(key -> [])))"
      }

      val two: Chunk[(ProducerRecord[String, String], RecordMetadata)] =
        Chunk(
          ProducerRecord("topic", "key", "value").withPartition(0).withTimestamp(0L) ->
            new RecordMetadata(new TopicPartition("topic", 0), 0L, 0, 0L, 0, 0),
          ProducerRecord("topic", "key", "value").withPartition(1).withTimestamp(0L) ->
            new RecordMetadata(new TopicPartition("topic", 1), 0L, 0, 0L, 0, 0)
        )

      assert {
        ProducerResult(two).toString == "ProducerResult(topic-0@0 -> ProducerRecord(topic = topic, partition = 0, timestamp = 0, key = key, value = value), topic-1@0 -> ProducerRecord(topic = topic, partition = 1, timestamp = 0, key = key, value = value))" &&
        ProducerResult(two).show == "ProducerResult(topic-0@0 -> ProducerRecord(topic = topic, partition = 0, timestamp = 0, key = key, value = value), topic-1@0 -> ProducerRecord(topic = topic, partition = 1, timestamp = 0, key = key, value = value))"
      }
    }
  }
}

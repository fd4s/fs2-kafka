/*
 * Copyright 2018-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

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
        ProducerResult(empty, 123).toString == "ProducerResult(<empty>, 123)" &&
        ProducerResult(empty, 123).show == ProducerResult(empty, 123).toString
      }

      val one: Chunk[(ProducerRecord[String, String], RecordMetadata)] =
        Chunk.singleton(
          ProducerRecord("topic", "key", "value")
            .withPartition(1)
            .withTimestamp(0L)
            .withHeaders(Headers(Header("key", Array[Byte]()))) ->
            new RecordMetadata(new TopicPartition("topic", 0), 0L, 0L, 0L, 0L, 0, 0)
        )

      assert {
        ProducerResult(one, 123).toString == "ProducerResult(topic-0@0 -> ProducerRecord(topic = topic, partition = 1, timestamp = 0, key = key, value = value, headers = Headers(key -> [])), 123)" &&
        ProducerResult(one, 123).show == "ProducerResult(topic-0@0 -> ProducerRecord(topic = topic, partition = 1, timestamp = 0, key = key, value = value, headers = Headers(key -> [])), 123)"
      }

      val two: Chunk[(ProducerRecord[String, String], RecordMetadata)] =
        Chunk(
          ProducerRecord("topic", "key", "value").withPartition(0).withTimestamp(0L) ->
            new RecordMetadata(new TopicPartition("topic", 0), 0L, 0L, 0L, 0L, 0, 0),
          ProducerRecord("topic", "key", "value").withPartition(1).withTimestamp(0L) ->
            new RecordMetadata(new TopicPartition("topic", 1), 0L, 0L, 0L, 0L, 0, 0)
        )

      assert {
        ProducerResult(two, 123).toString == "ProducerResult(topic-0@0 -> ProducerRecord(topic = topic, partition = 0, timestamp = 0, key = key, value = value), topic-1@0 -> ProducerRecord(topic = topic, partition = 1, timestamp = 0, key = key, value = value), 123)" &&
        ProducerResult(two, 123).show == "ProducerResult(topic-0@0 -> ProducerRecord(topic = topic, partition = 0, timestamp = 0, key = key, value = value), topic-1@0 -> ProducerRecord(topic = topic, partition = 1, timestamp = 0, key = key, value = value), 123)"
      }
    }
  }
}

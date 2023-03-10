/*
 * Copyright 2018-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.IO
import cats.instances.list._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import fs2.Chunk

class TransactionalProducerRecordsSpec extends BaseSpec {
  describe("TransactionalProducerRecords") {
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
        TransactionalProducerRecords
          .one(CommittableProducerRecords.one(record, offset), 123)
          .toString == "TransactionalProducerRecords(CommittableProducerRecords(ProducerRecord(topic = topic, key = key, value = value), CommittableOffset(topic-1 -> 1, the-group)), 123)" &&
        TransactionalProducerRecords
          .one(CommittableProducerRecords.one(record, offset))
          .toString == "TransactionalProducerRecords(CommittableProducerRecords(ProducerRecord(topic = topic, key = key, value = value), CommittableOffset(topic-1 -> 1, the-group)), ())"
      }
    }

    it("should be able to create with multiple records") {
      val records = List(
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
        TransactionalProducerRecords
          .one(CommittableProducerRecords(records, offset), 123)
          .toString == "TransactionalProducerRecords(CommittableProducerRecords(ProducerRecord(topic = topic, key = key, value = value), ProducerRecord(topic = topic2, key = key2, value = value2), CommittableOffset(topic-1 -> 1, the-group)), 123)" &&
        TransactionalProducerRecords
          .one(CommittableProducerRecords(records, offset))
          .toString == "TransactionalProducerRecords(CommittableProducerRecords(ProducerRecord(topic = topic, key = key, value = value), ProducerRecord(topic = topic2, key = key2, value = value2), CommittableOffset(topic-1 -> 1, the-group)), ())"
      }
    }

    it("should be able to create with zero records") {
      assert {
        TransactionalProducerRecords[IO, Int, String, String](Chunk.empty, 123).toString == "TransactionalProducerRecords(<empty>, 123)" &&
        TransactionalProducerRecords[IO, String, String](Chunk.empty).toString == "TransactionalProducerRecords(<empty>, ())"
      }
    }
  }
}

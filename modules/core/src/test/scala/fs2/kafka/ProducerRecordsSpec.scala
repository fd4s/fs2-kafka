/*
 * Copyright 2018-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.syntax.all._
import fs2.Chunk

final class ProducerRecordsSpec extends BaseSpec {
  describe("ProducerRecords") {
    it("should be able to create with one record") {
      val record = ProducerRecord("topic", "key", "value")

      assert {
        ProducerRecords
          .one[Int, String, String](record, 123)
          .toString == "ProducerRecords(ProducerRecord(topic = topic, key = key, value = value), 123)" &&
        ProducerRecords
          .one[Int, String, String](record, 123)
          .show == "ProducerRecords(ProducerRecord(topic = topic, key = key, value = value), 123)" &&
        ProducerRecords
          .one[String, String](record)
          .toString == "ProducerRecords(ProducerRecord(topic = topic, key = key, value = value), ())" &&
        ProducerRecords
          .one[String, String](record)
          .show == "ProducerRecords(ProducerRecord(topic = topic, key = key, value = value), ())"
      }
    }

    it("should be able to create with multiple records") {
      val records = List(ProducerRecord("topic", "key", "value"))
      assert {
        ProducerRecords[List, Int, String, String](records, 123).toString == "ProducerRecords(ProducerRecord(topic = topic, key = key, value = value), 123)" &&
        ProducerRecords[List, Int, String, String](records, 123).show == "ProducerRecords(ProducerRecord(topic = topic, key = key, value = value), 123)" &&
        ProducerRecords[List, String, String](records).toString == "ProducerRecords(ProducerRecord(topic = topic, key = key, value = value), ())" &&
        ProducerRecords[List, String, String](records).show == "ProducerRecords(ProducerRecord(topic = topic, key = key, value = value), ())"
      }
    }

    it("should be able to create with passthrough only") {
      assert {
        ProducerRecords[List, Int, String, String](Nil, 123).toString == "ProducerRecords(<empty>, 123)" &&
        ProducerRecords[List, Int, String, String](Nil, 123).show == "ProducerRecords(<empty>, 123)"
      }
    }
  }

  it("should be able to create with multiple records in a chunk") {
    val records = Chunk(ProducerRecord("topic", "key", "value"))

    assert {
      ProducerRecords
        .chunk[Int, String, String](records, 123)
        .toString == "ProducerRecords(ProducerRecord(topic = topic, key = key, value = value), 123)" &&
      ProducerRecords
        .chunk[Int, String, String](records, 123)
        .show == "ProducerRecords(ProducerRecord(topic = topic, key = key, value = value), 123)" &&
      ProducerRecords
        .chunk[String, String](records)
        .toString == "ProducerRecords(ProducerRecord(topic = topic, key = key, value = value), ())" &&
      ProducerRecords
        .chunk[String, String](records)
        .show == "ProducerRecords(ProducerRecord(topic = topic, key = key, value = value), ())"
    }
  }
}

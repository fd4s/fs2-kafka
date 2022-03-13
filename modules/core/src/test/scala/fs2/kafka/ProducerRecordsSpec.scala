package fs2.kafka

import cats.syntax.all._

final class ProducerRecordsSpec extends BaseSpec {
  describe("ProducerRecords") {
    it("should be able to create with one record") {
      val record = ProducerRecord("topic", "key", "value")

      assert {
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
        ProducerRecords[List, String, String](records).toString == "ProducerRecords(ProducerRecord(topic = topic, key = key, value = value), ())" &&
        ProducerRecords[List, String, String](records).show == "ProducerRecords(ProducerRecord(topic = topic, key = key, value = value), ())"
      }
    }
  }
}

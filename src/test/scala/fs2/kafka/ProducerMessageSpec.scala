package fs2.kafka

import cats.implicits._
import org.apache.kafka.clients.producer.ProducerRecord

final class ProducerMessageSpec extends BaseSpec {
  describe("ProducerMessage") {
    it("should be able to create with one record") {
      val record = new ProducerRecord("topic", "key", "value")

      assert {
        ProducerMessage
          .one[String, String, Int](record, 123)
          .toString == "ProducerMessage(ProducerRecord(topic=topic, partition=null, headers=RecordHeaders(headers = [], isReadOnly = false), key=key, value=value, timestamp=null), 123)" &&
        ProducerMessage
          .one[String, String, Int](record, 123)
          .show == "ProducerMessage(ProducerRecord(topic = topic, partition = null, headers = Headers(<empty>), key = key, value = value, timestamp = null), 123)" &&
        ProducerMessage
          .one[String, String](record)
          .toString == "ProducerMessage(ProducerRecord(topic=topic, partition=null, headers=RecordHeaders(headers = [], isReadOnly = false), key=key, value=value, timestamp=null), ())" &&
        ProducerMessage
          .one[String, String](record)
          .show == "ProducerMessage(ProducerRecord(topic = topic, partition = null, headers = Headers(<empty>), key = key, value = value, timestamp = null), ())"
      }
    }

    it("should be able to create with multiple records") {
      val records = List(new ProducerRecord("topic", "key", "value"))
      assert {
        ProducerMessage[List, String, String, Int](records, 123).toString == "ProducerMessage(ProducerRecord(topic=topic, partition=null, headers=RecordHeaders(headers = [], isReadOnly = false), key=key, value=value, timestamp=null), 123)" &&
        ProducerMessage[List, String, String, Int](records, 123).show == "ProducerMessage(ProducerRecord(topic = topic, partition = null, headers = Headers(<empty>), key = key, value = value, timestamp = null), 123)" &&
        ProducerMessage[List, String, String](records).toString == "ProducerMessage(ProducerRecord(topic=topic, partition=null, headers=RecordHeaders(headers = [], isReadOnly = false), key=key, value=value, timestamp=null), ())" &&
        ProducerMessage[List, String, String](records).show == "ProducerMessage(ProducerRecord(topic = topic, partition = null, headers = Headers(<empty>), key = key, value = value, timestamp = null), ())" &&
        ProducerMessage[List]
          .of(records, 123)
          .toString == "ProducerMessage(ProducerRecord(topic=topic, partition=null, headers=RecordHeaders(headers = [], isReadOnly = false), key=key, value=value, timestamp=null), 123)" &&
        ProducerMessage[List]
          .of(records, 123)
          .show == "ProducerMessage(ProducerRecord(topic = topic, partition = null, headers = Headers(<empty>), key = key, value = value, timestamp = null), 123)" &&
        ProducerMessage[List]
          .of(records)
          .toString == "ProducerMessage(ProducerRecord(topic=topic, partition=null, headers=RecordHeaders(headers = [], isReadOnly = false), key=key, value=value, timestamp=null), ())" &&
        ProducerMessage[List]
          .of(records)
          .show == "ProducerMessage(ProducerRecord(topic = topic, partition = null, headers = Headers(<empty>), key = key, value = value, timestamp = null), ())"
      }
    }

    it("should be able to create with passthrough only") {
      assert {
        ProducerMessage[List, String, String, Int](Nil, 123).toString == "ProducerMessage(<empty>, 123)" &&
        ProducerMessage[List, String, String, Int](Nil, 123).show == "ProducerMessage(<empty>, 123)"
      }
    }
  }
}

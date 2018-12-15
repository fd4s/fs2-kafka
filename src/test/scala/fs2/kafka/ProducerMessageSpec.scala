package fs2.kafka

import cats.Id
import cats.implicits._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Header
import scala.collection.JavaConverters._

final class ProducerMessageSpec extends BaseSpec {
  describe("ProducerMessage") {
    it("should be able to create with one record") {
      def header(_key: String, _value: String): Header =
        new Header {
          override def key(): String = _key
          override def value(): Array[Byte] = _value.getBytes
        }

      val record = new ProducerRecord("topic", "key", "value")

      val headers1 = List(header("key1", "value1"))
      val recordHeader1 = new ProducerRecord("topic", 0, 0L, "key", "value", headers1.asJava)

      val headers2 = List(header("key1", "value1"), header("key2", "value2"))
      val recordHeader2 = new ProducerRecord("topic", 0, 0L, "key", "value", headers2.asJava)

      assert {
        ProducerMessage
          .single[Id, String, String, Int](record, 123)
          .toString == "ProducerMessage(ProducerRecord(topic=topic, partition=null, headers=RecordHeaders(headers = [], isReadOnly = false), key=key, value=value, timestamp=null), 123)" &&
        ProducerMessage
          .single[Id, String, String, Int](record, 123)
          .show == "ProducerMessage(ProducerRecord(topic = topic, partition = null, headers = Headers(<empty>), key = key, value = value, timestamp = null), 123)" &&
        ProducerMessage
          .single[Id, String, String](record)
          .toString == "ProducerMessage(ProducerRecord(topic=topic, partition=null, headers=RecordHeaders(headers = [], isReadOnly = false), key=key, value=value, timestamp=null), ())" &&
        ProducerMessage
          .single[Id, String, String](record)
          .show == "ProducerMessage(ProducerRecord(topic = topic, partition = null, headers = Headers(<empty>), key = key, value = value, timestamp = null), ())" &&
        ProducerMessage
          .single[Id]
          .of(record, 123)
          .toString == "ProducerMessage(ProducerRecord(topic=topic, partition=null, headers=RecordHeaders(headers = [], isReadOnly = false), key=key, value=value, timestamp=null), 123)" &&
        ProducerMessage
          .single[Id]
          .of(record, 123)
          .show == "ProducerMessage(ProducerRecord(topic = topic, partition = null, headers = Headers(<empty>), key = key, value = value, timestamp = null), 123)" &&
        ProducerMessage
          .single[Id]
          .of(record)
          .toString == "ProducerMessage(ProducerRecord(topic=topic, partition=null, headers=RecordHeaders(headers = [], isReadOnly = false), key=key, value=value, timestamp=null), ())" &&
        ProducerMessage
          .single[Id]
          .of(record)
          .show == "ProducerMessage(ProducerRecord(topic = topic, partition = null, headers = Headers(<empty>), key = key, value = value, timestamp = null), ())" &&
        ProducerMessage
          .single[Id]
          .of(recordHeader1)
          .show == "ProducerMessage(ProducerRecord(topic = topic, partition = 0, headers = Headers(key1 -> [118, 97, 108, 117, 101, 49]), key = key, value = value, timestamp = 0), ())" &&
        ProducerMessage
          .single[Id]
          .of(recordHeader2)
          .show == "ProducerMessage(ProducerRecord(topic = topic, partition = 0, headers = Headers(key1 -> [118, 97, 108, 117, 101, 49], key2 -> [118, 97, 108, 117, 101, 50]), key = key, value = value, timestamp = 0), ())"
      }
    }

    it("should be able to create with multiple records") {
      val records = List(new ProducerRecord("topic", "key", "value"))
      assert {
        ProducerMessage
          .multiple[List, String, String, Int](records, 123)
          .toString == "ProducerMessage(ProducerRecord(topic=topic, partition=null, headers=RecordHeaders(headers = [], isReadOnly = false), key=key, value=value, timestamp=null), 123)" &&
        ProducerMessage
          .multiple[List, String, String, Int](records, 123)
          .show == "ProducerMessage(ProducerRecord(topic = topic, partition = null, headers = Headers(<empty>), key = key, value = value, timestamp = null), 123)" &&
        ProducerMessage
          .multiple[List, String, String](records)
          .toString == "ProducerMessage(ProducerRecord(topic=topic, partition=null, headers=RecordHeaders(headers = [], isReadOnly = false), key=key, value=value, timestamp=null), ())" &&
        ProducerMessage
          .multiple[List, String, String](records)
          .show == "ProducerMessage(ProducerRecord(topic = topic, partition = null, headers = Headers(<empty>), key = key, value = value, timestamp = null), ())" &&
        ProducerMessage
          .multiple[List]
          .of(records, 123)
          .toString == "ProducerMessage(ProducerRecord(topic=topic, partition=null, headers=RecordHeaders(headers = [], isReadOnly = false), key=key, value=value, timestamp=null), 123)" &&
        ProducerMessage
          .multiple[List]
          .of(records, 123)
          .show == "ProducerMessage(ProducerRecord(topic = topic, partition = null, headers = Headers(<empty>), key = key, value = value, timestamp = null), 123)" &&
        ProducerMessage
          .multiple[List]
          .of(records)
          .toString == "ProducerMessage(ProducerRecord(topic=topic, partition=null, headers=RecordHeaders(headers = [], isReadOnly = false), key=key, value=value, timestamp=null), ())" &&
        ProducerMessage
          .multiple[List]
          .of(records)
          .show == "ProducerMessage(ProducerRecord(topic = topic, partition = null, headers = Headers(<empty>), key = key, value = value, timestamp = null), ())"
      }
    }

    it("should be able to create with passthrough only") {
      assert {
        ProducerMessage
          .passthrough[List, String, String, Int](123)
          .toString == "ProducerMessage(<empty>, 123)" &&
        ProducerMessage
          .passthrough[List, String, String, Int](123)
          .show == "ProducerMessage(<empty>, 123)" &&
        ProducerMessage
          .passthrough[List]
          .withKeyAndValue[String, String]
          .of(123)
          .toString == "ProducerMessage(<empty>, 123)" &&
        ProducerMessage
          .passthrough[List]
          .withKeyAndValue[String, String]
          .of(123)
          .show == "ProducerMessage(<empty>, 123)" &&
        ProducerMessage
          .passthrough[List]
          .of(123)
          .toString == "ProducerMessage(<empty>, 123)"
      }
    }
  }
}

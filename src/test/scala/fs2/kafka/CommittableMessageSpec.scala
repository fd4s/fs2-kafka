package fs2.kafka

import cats.Id
import cats.implicits._
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

final class CommittableMessageSpec extends BaseSpec {
  describe("CommittableMessage") {
    it("should have a Show instance and matching toString") {
      val message =
        CommittableMessage(
          new ConsumerRecord("topic", 0, 0L, "key", "value"),
          CommittableOffset[Id](
            topicPartition = new TopicPartition("topic", 0),
            offsetAndMetadata = new OffsetAndMetadata(0L),
            commit = _ => ()
          )
        )

      assert {
        message.toString == "CommittableMessage(ConsumerRecord(topic = topic, partition = 0, offset = 0, NoTimestampType = -1, serialized key size = -1, serialized value size = -1, headers = RecordHeaders(headers = [], isReadOnly = false), key = key, value = value), CommittableOffset(topic-0 -> 0))" &&
        message.show == "CommittableMessage(ConsumerRecord(topic = topic, partition = 0, offset = 0, NoTimestampType = -1, serialized key size = -1, serialized value size = -1, headers = Headers(<empty>), key = key, value = value), CommittableOffset(topic-0 -> 0))"
      }
    }
  }
}

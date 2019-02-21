package fs2.kafka

import cats.implicits._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

final class CommittableMessageSpec extends BaseSpec {
  describe("CommittableMessage") {
    it("should have a Show instance and matching toString") {
      val message =
        CommittableMessage(
          ConsumerRecord("topic", 0, 0L, "key", "value"),
          CommittableOffset[Id](
            topicPartition = new TopicPartition("topic", 0),
            offsetAndMetadata = new OffsetAndMetadata(0L),
            commit = _ => ()
          )
        )

      assert {
        message.toString == "CommittableMessage(ConsumerRecord(topic = topic, partition = 0, offset = 0, key = key, value = value), CommittableOffset(topic-0 -> 0))" &&
        message.show == "CommittableMessage(ConsumerRecord(topic = topic, partition = 0, offset = 0, key = key, value = value), CommittableOffset(topic-0 -> 0))"
      }
    }
  }
}

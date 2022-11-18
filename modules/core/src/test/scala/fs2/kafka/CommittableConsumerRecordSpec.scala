package fs2.kafka

import cats.effect.IO
import cats.syntax.all._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

final class CommittableConsumerRecordSpec extends BaseSpec {
  describe("CommittableConsumerRecord") {
    it("should have a Show instance and matching toString") {
      val record: CommittableConsumerRecord[IO, String, String] =
        CommittableConsumerRecord(
          ConsumerRecord("topic", 0, 0L, "key", "value"),
          CommittableOffset[IO](
            topicPartition = new TopicPartition("topic", 0),
            offsetAndMetadata = new OffsetAndMetadata(0L),
            consumerGroupId = Some("the-group"),
            commit = _ => IO.unit
          )
        )

      assert {
        record.toString == "CommittableConsumerRecord(ConsumerRecord(topic = topic, partition = 0, offset = 0, key = key, value = value), CommittableOffset(topic-0 -> 0, the-group))" &&
        record.show == "CommittableConsumerRecord(ConsumerRecord(topic = topic, partition = 0, offset = 0, key = key, value = value), CommittableOffset(topic-0 -> 0, the-group))"
      }
    }
  }
}

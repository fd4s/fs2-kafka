package fs2.kafka

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

import scala.concurrent.duration._

final class CommitTimeoutExceptionSpec extends BaseSpec {
  describe("CommitTimeoutException") {
    it("should have expected message and toString") {
      val exception =
        CommitTimeoutException(
          10.seconds,
          Map(
            new TopicPartition("topic", 0) -> new OffsetAndMetadata(0L),
            new TopicPartition("topic", 1) -> new OffsetAndMetadata(1L)
          )
        )

      assert {
        exception.getMessage == "offset commit timeout after 10 seconds for offsets: topic-0 -> 0, topic-1 -> 1" &&
        exception.toString == "fs2.kafka.CommitTimeoutException: offset commit timeout after 10 seconds for offsets: topic-0 -> 0, topic-1 -> 1"
      }
    }
  }
}

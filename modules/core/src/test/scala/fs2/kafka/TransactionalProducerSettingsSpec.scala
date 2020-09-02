package fs2.kafka

import cats.effect.IO
import org.apache.kafka.common.TopicPartition

final class TransactionalProducerSettingsSpec extends BaseSpec {
  describe("TransactionalProducerSettings") {
    val baseSettings =
      ProducerSettings[IO, String, String]

    it("should accept transactional.id") {
      forAll { id: String =>
        TransactionalProducerSettings(id, baseSettings).transactionalId shouldBe id
      }
    }

    it("should accept application id and partition") {
      forAll { (applicationId: String, partition: TopicPartition) =>
        val actual =
          TransactionalProducerSettings(applicationId, partition, baseSettings).transactionalId

        val expected =
          s"${applicationId}_${partition.topic}_${partition.partition}"

        actual shouldBe expected
      }
    }
  }
}

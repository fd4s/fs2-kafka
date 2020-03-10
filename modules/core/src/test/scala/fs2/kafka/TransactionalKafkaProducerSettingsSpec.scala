package fs2.kafka

import cats.effect.IO
import org.apache.kafka.common.TopicPartition
import org.scalatest.{EitherValues, OptionValues}

class TransactionalKafkaProducerSettingsSpec extends BaseKafkaSpec with OptionValues with EitherValues {

  describe("TransactionalProducerSettings") {

    val baseSettings = ProducerSettings(Serializer[IO, String], Serializer[IO, String])

    it("should provide basic apply") {
      TransactionalProducerSettings("id", baseSettings).transactionalId shouldBe "id"
    }

    it("should uniquely encode a TopicPartition into a transactional ID") {
      TransactionalProducerSettings("my-app", new TopicPartition("foo", 0), baseSettings).transactionalId shouldBe "my-app_foo_0"
      TransactionalProducerSettings("my-app", new TopicPartition("bar", 1), baseSettings).transactionalId shouldBe "my-app_bar_1"
    }
  }
}

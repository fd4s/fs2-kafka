package fs2.kafka

import cats.effect.IO

final class PackageSpec extends BaseKafkaSpec {
  describe("creating admin clients") {
    it("should support defined syntax") {
      val settings =
        AdminClientSettings.Default

      adminClientResource[IO](settings)
      adminClientStream[IO](settings)
    }
  }

  describe("creating consumers") {
    it("should support defined syntax") {
      val settings =
        ConsumerSettings[IO, String, String]

      consumerResource[IO, String, String](settings)
      consumerResource[IO].toString should startWith("ConsumerResource$")
      consumerResource[IO].using(settings)

      consumerStream[IO, String, String](settings)
      consumerStream[IO].toString should startWith("ConsumerStream$")
      consumerStream[IO].using(settings)
    }
  }

  describe("creating producers") {
    it("should support defined syntax") {
      val settings =
        ProducerSettings[IO, String, String]

      producerResource[IO, String, String](settings)
      producerResource[IO].toString should startWith("ProducerResource$")
      producerResource[IO].using(settings)

      producerStream[IO, String, String](settings)
      producerStream[IO].toString should startWith("ProducerStream$")
      producerStream[IO].using(settings)
    }
  }

  describe("creating consumer execution contexts") {
    it("should support defined syntax") {
      consumerExecutionContextResource[IO]
      consumerExecutionContextResource[IO](1)

      consumerExecutionContextStream[IO]
      consumerExecutionContextStream[IO](1)
    }
  }

  describe("creating producer execution contexts") {
    it("should support defined syntax") {
      producerExecutionContextResource[IO]
      producerExecutionContextResource[IO](1)

      producerExecutionContextStream[IO]
      producerExecutionContextStream[IO](1)
    }
  }
}

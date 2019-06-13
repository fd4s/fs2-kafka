package fs2.kafka

import cats.effect.IO

final class PackageSpec extends BaseAsyncSpec {
  describe("creating admin clients") {
    it("should support defined syntax") {
      val settings =
        AdminClientSettings[IO]

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

  describe("creating transactional producers") {
    it("should support defined syntax") {
      val settings =
        ProducerSettings[IO, String, String]

      transactionalProducerResource[IO, String, String](settings)
      transactionalProducerResource[IO].toString should startWith("TransactionalProducerResource$")
      transactionalProducerResource[IO].using(settings)

      transactionalProducerStream[IO, String, String](settings)
      transactionalProducerStream[IO].toString should startWith("TransactionalProducerStream$")
      transactionalProducerStream[IO].using(settings)
    }
  }
}

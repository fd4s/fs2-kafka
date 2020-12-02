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

  describe("creating transactional producers") {
    it("should support defined syntax") {
      val settings = TransactionalProducerSettings("id", ProducerSettings[IO, String, String])

      transactionalProducerResource[IO, String, String](settings)
      transactionalProducerResource[IO].toString should startWith("TransactionalProducerResource$")
      transactionalProducerResource[IO].using(settings)

      transactionalProducerStream[IO, String, String](settings)
      transactionalProducerStream[IO].toString should startWith("TransactionalProducerStream$")
      transactionalProducerStream[IO].using(settings)
    }
  }
}

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

}

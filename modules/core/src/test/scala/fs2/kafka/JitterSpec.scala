package fs2.kafka

import cats.effect.IO

final class JitterSpec extends BaseSpec {
  describe("Jitter#default") {
    it("should always apply jitter on values") {
      val jitter: Jitter[IO] = Jitter.default[IO].unsafeRunSync()

      forAll { n: Double =>
        whenever(!n.isNaN) {
          val jittered = jitter.withJitter(n).unsafeRunSync()

          if (n == 0 || n.isInfinite) assert(jittered == n)
          else if (n > 0) assert(0 <= jittered && jittered < n)
          else assert(n < jittered && jittered <= 0)
        }
      }
    }
  }

  describe("Jitter#none") {
    it("should never apply jitter on values") {
      val jitter: Jitter[Id] = Jitter.none

      forAll { n: Double =>
        whenever(!n.isNaN) {
          assert(jitter.withJitter(n) == n)
        }
      }
    }
  }
}

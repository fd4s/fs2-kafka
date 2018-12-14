package fs2.kafka.internal

import java.time.temporal.ChronoUnit.MICROS

import fs2.kafka.BaseSpec
import fs2.kafka.internal.syntax._

import scala.concurrent.duration._

final class SyntaxSpec extends BaseSpec {
  describe("FiniteDuration#asJava") {
    it("should convert days") { assert(1.day.asJava == java.time.Duration.ofDays(1)) }
    it("should convert hours") { assert(1.hour.asJava == java.time.Duration.ofHours(1)) }
    it("should convert minutes") { assert(1.minute.asJava == java.time.Duration.ofMinutes(1)) }
    it("should convert seconds") { assert(1.second.asJava == java.time.Duration.ofSeconds(1)) }
    it("should convert millis") { assert(1.milli.asJava == java.time.Duration.ofMillis(1)) }
    it("should convert micros") { assert(1.micro.asJava == java.time.Duration.of(1, MICROS)) }
    it("should convert nanos") { assert(1.nanos.asJava == java.time.Duration.ofNanos(1)) }
  }

  describe("Map#filterKeysStrictValuesList") {
    it("should be the same as filterKeys(p).values.toList") {
      forAll { (m: Map[Int, Int], p: Int => Boolean) =>
        assert {
          m.filterKeysStrictValuesList(p) == m.filterKeys(p).values.toList
        }
      }
    }
  }
}

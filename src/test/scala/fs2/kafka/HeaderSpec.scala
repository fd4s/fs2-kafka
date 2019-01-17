package fs2.kafka

import cats.syntax.show._

final class HeaderSpec extends BaseSpec {
  describe("Header") {
    it("toString should be Header(key -> value)") {
      assert {
        Header("key", Array(1.toByte, 2.toByte)).toString == "Header(key -> [1, 2])"
      }
    }

    it("should have consistent toString and Show") {
      assert {
        val header = Header("key", Array(1.toByte, 2.toByte))
        header.show == header.toString
      }
    }

    it("should return the key with key") {
      assert(Header("key", Array()).key == "key")
    }

    it("should return the value with value") {
      val value = Array[Byte]()
      assert(Header("key", value).value == value)
    }
  }
}

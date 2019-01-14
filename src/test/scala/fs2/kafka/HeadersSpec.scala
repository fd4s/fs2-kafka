package fs2.kafka
import cats.data.Chain

final class HeadersSpec extends BaseSpec {
  describe("Headers#empty") {
    it("should have toString as Headers()") {
      assert(Headers.empty.toString == "Headers()")
    }

    it("should return true for isEmpty") {
      assert(Headers.empty.isEmpty)
    }

    it("should return empty Chain from toChain") {
      assert(Headers.empty.toChain.isEmpty)
    }

    it("should create a new Headers from append(Header)") {
      assert {
        val header = Header("key", Array())
        Headers.empty.append(header).toChain == Chain.one(header)
      }
    }

    it("should create a new Headers from append(key, value)") {
      assert {
        Headers.empty.append("key", Array()).toChain.size == 1
      }
    }
  }

  describe("Headers (non-empty)") {
    it("should not be empty") {
      assert(!Headers(Header("key", Array())).isEmpty)
    }

    it("should include the headers in toString") {
      val headers = Chain(Header("key1", Array()), Header("key2", Array()))
      assert(Headers.fromChain(headers).toString == "Headers(key1 -> [], key2 -> [])")
    }

    it("should append one more header with append(Header)") {
      val header1 = Header("key1", Array())
      val header2 = Header("key2", Array())

      Headers(header1).append(header2).toChain == Chain(header1, header2)
    }

    it("should append one more header with append(key, value)") {
      Headers(Header("key1", Array())).append("key2", Array()).toChain.size == 2
    }
  }

  describe("Headers#apply") {
    it("returns empty for no headers") {
      assert(Headers() == Headers.empty)
    }

    it("returns non-empty for at least one header") {
      val header = Header("key", Array())
      Headers(header).toChain == Chain.one(header)
    }
  }

  describe("Headers#fromChain") {
    it("returns empty for empty chain") {
      assert(Headers.fromChain(Chain.empty) == Headers.empty)
    }

    it("returns non-empty for non-empty chain") {
      val headers = Chain(Header("key1", Array()), Header("key2", Array()))
      assert(Headers.fromChain(headers).toChain == headers)
    }
  }
}

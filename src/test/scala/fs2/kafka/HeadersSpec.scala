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

    it("should return false for nonEmpty") {
      assert(!Headers.empty.nonEmpty)
    }

    it("should not find any key") {
      assert(Headers.empty("key").isEmpty)
    }

    it("should concat empty") {
      assert(Headers.empty.concat(Headers.empty).isEmpty)
    }

    it("should concat non-empty") {
      val headers = Header("key", Array()).headers
      assert(Headers.empty.concat(headers).toChain.size == 1)
    }

    it("should return empty Chain from toChain") {
      assert(Headers.empty.toChain.isEmpty)
    }

    it("should return empty from asJava") {
      assert(Headers.empty.asJava.toArray.isEmpty)
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

    it("should be non empty") {
      assert(Headers(Header("key", Array())).nonEmpty)
    }

    it("should find an existing key") {
      assert(Headers(Header("key", Array()))("key").isDefined)
    }

    it("should find the first key") {
      val headers =
        Headers(
          Header("key", Array(1)),
          Header("key", Array(2))
        )

      assert(headers("key").map(_.value.head) == Some(1.toByte))
    }

    it("should return non-empty from asJava") {
      assert(Headers(Header("key", Array())).asJava.toArray.size == 1)
    }

    it("should concat empty") {
      val headers = Header("key", Array()).headers
      assert(headers.concat(Headers.empty).toChain.size == 1)
    }

    it("should concat non-empty") {
      val headers = Header("key", Array()).headers
      assert(headers.concat(headers).toChain.size == 2)
    }

    it("should include the headers in toString") {
      val headers = Chain(Header("key1", Array()), Header("key2", Array()))
      assert(Headers.fromChain(headers).toString == "Headers(key1 -> [], key2 -> [])")
    }

    it("should append one more header with append(Header)") {
      val header1 = Header("key1", Array())
      val header2 = Header("key2", Array())

      assert(Headers(header1).append(header2).toChain.size == 2)
    }

    it("should append one more header with append(key, value)") {
      assert(Headers(Header("key1", Array())).append("key2", Array()).toChain.size == 2)
    }
  }

  describe("Headers#apply") {
    it("returns empty for no headers") {
      assert(Headers() == Headers.empty)
    }

    it("returns non-empty for at least one header") {
      val header = Header("key", Array())
      assert(Headers(header).toChain == Chain.one(header))
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

  describe("Headers#fromSeq") {
    it("returns empty for empty seq") {
      assert(Headers.fromSeq(Seq.empty) == Headers.empty)
    }

    it("returns non-empty for non-empty seq") {
      val headers = Seq(Header("key1", Array()), Header("key2", Array()))
      assert(Headers.fromSeq(headers).toChain == Chain.fromSeq(headers))
    }
  }

  describe("Headers#fromIterable") {
    it("returns empty for empty iterable") {
      assert(Headers.fromIterable(List.empty) == Headers.empty)
    }

    it("returns non-empty for non-empty iterable") {
      val headers = List(Header("key1", Array()), Header("key2", Array()))
      assert(Headers.fromIterable(headers).toChain == Chain.fromSeq(headers))
    }
  }
}

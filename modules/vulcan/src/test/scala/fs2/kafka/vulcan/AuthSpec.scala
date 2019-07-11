package fs2.kafka.vulcan

import org.scalatest.funspec.AnyFunSpec
import org.scalatestplus.scalacheck._

final class AuthSpec extends AnyFunSpec with ScalaCheckPropertyChecks {
  describe("Auth.Basic") {
    it("should include the username in toString") {
      forAll { (username: String, password: String) =>
        val auth = Auth.Basic(username, password)
        assert(auth.toString.contains(username))
      }
    }

    it("should not include the password in toString") {
      forAll { (username: String, password: String) =>
        whenever(!username.contains(password)) {
          val auth = Auth.Basic(username, password)
          assert(!auth.toString.contains(password))
        }
      }
    }
  }

  describe("Auth.None") {
    it("should have None as toString") {
      assert(Auth.None.toString == "None")
    }
  }
}

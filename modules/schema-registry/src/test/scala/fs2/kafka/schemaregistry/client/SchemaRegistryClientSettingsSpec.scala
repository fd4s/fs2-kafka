package fs2.kafka.schemaregistry.client

import org.scalatest.funspec.AnyFunSpec
import org.scalatestplus.scalacheck._
import cats.syntax.all._

final class SchemaRegistryClientSettingsSpec extends AnyFunSpec with ScalaCheckPropertyChecks {

  val settings: SchemaRegistryClientSettings =
    SchemaRegistryClientSettings("baseUrl")

  describe("SchemaRegistryClientSettings") {
    it("should provide withMaxCacheSize") {
      assert {
        settings
          .withMaxCacheSize(10)
          .maxCacheSize == 10
      }
    }

    it("should provide withAuth(Auth.Basic)") {
      forAll { (username: String, password: String) =>
        val settingsWithAuth =
          settings.withAuth(Auth.Basic(username, password))

        assert {
          settingsWithAuth.properties
            .get("basic.auth.credentials.source")
            .contains("USER_INFO")
        }

        assert {
          settingsWithAuth.properties
            .get("schema.registry.basic.auth.user.info")
            .contains(s"$username:$password")
        }
      }
    }

    it("should provide withAuth(Auth.Bearer)") {
      forAll { (token: String) =>
        val settingsWithAuth =
          settings.withAuth(Auth.Bearer(token))

        assert {
          settingsWithAuth.properties
            .get("bearer.auth.credentials.source")
            .contains("STATIC_TOKEN")
        }

        assert {
          settingsWithAuth.properties
            .get("bearer.auth.token")
            .contains(token)
        }
      }
    }

    it("should provide withAuth(Auth.None)") {
      val settingsWithAuth =
        settings.withAuth(Auth.None)

      assert {
        !settingsWithAuth.properties.contains("basic.auth.credentials.source")
      }

      assert {
        !settingsWithAuth.properties.contains("schema.registry.basic.auth.user.info")
      }
    }

    it("should provide withProperty") {
      forAll { (key: String, value: String) =>
        settings
          .withProperty(key, value)
          .properties
          .get(key)
          .contains(value)
      }
    }

    it("should provide withProperties") {
      forAll { (key: String, value: String) =>
        settings
          .withProperties(key -> value)
          .properties
          .get(key)
          .contains(value)
      }
    }

    it("should provide withProperties(Map)") {
      forAll { (key: String, value: String) =>
        settings
          .withProperties(Map(key -> value))
          .properties
          .get(key)
          .contains(value)
      }
    }

    it("should provide toString") {
      assert {
        settings.toString == "SchemaRegistryClientSettings(baseUrl = baseUrl, maxCacheSize = 1000)"
      }
    }

    it("should provide Show") {
      assert {
        settings.show == "SchemaRegistryClientSettings(baseUrl = baseUrl, maxCacheSize = 1000)"
      }
    }

    it("should provide Eq") {

      val s1 = SchemaRegistryClientSettings("baseUrl")
        .withAuth(Auth.Basic("user", "password"))
        .withMaxCacheSize(100)
        .withProperties("TEST" -> "FOO")

      val s2 = SchemaRegistryClientSettings("baseUrl")
        .withAuth(Auth.Basic("user", "password"))
        .withMaxCacheSize(100)
        .withProperties("TEST" -> "FOO")

      assert {
        s1.eqv(s2)
      }
    }

  }
}

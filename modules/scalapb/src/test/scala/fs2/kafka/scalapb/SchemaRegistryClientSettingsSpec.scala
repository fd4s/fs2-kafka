package fs2.kafka.scalapb

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits._
import org.scalatest.funspec.AnyFunSpec
import org.scalatestplus.scalacheck._

final class SchemaRegistryClientSettingsSpec extends AnyFunSpec with ScalaCheckPropertyChecks {
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
        settingsWithAuth.properties
          .get("basic.auth.credentials.source")
          .isEmpty
      }

      assert {
        settingsWithAuth.properties
          .get("schema.registry.basic.auth.user.info")
          .isEmpty
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

    it("should provide withCreateSchemaRegistryClient") {
      assert {
        settings
          .withCreateSchemaRegistryClient {
            case _ => IO.raiseError(new RuntimeException)
          }
          .createSchemaRegistryClient
          .attempt
          .unsafeRunSync()
          .isLeft
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
  }

  val settings: SchemaRegistryClientSettings[IO] =
    SchemaRegistryClientSettings("baseUrl")
}

package fs2.kafka

import cats.effect.IO
import cats.implicits._
import org.apache.kafka.clients.admin.AdminClientConfig
import scala.concurrent.duration._
import cats.effect.Resource

final class AdminClientSettingsSpec extends BaseSpec {
  describe("AdminClientSettings") {
    it("should provide withBootstrapServers") {
      assert {
        settings
          .withBootstrapServers("localhost")
          .properties(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG)
          .contains("localhost")
      }
    }

    it("should provide withClientId") {
      assert {
        settings
          .withClientId("client")
          .properties(AdminClientConfig.CLIENT_ID_CONFIG)
          .contains("client")
      }
    }

    it("should provide withReconnectBackoff") {
      assert {
        settings
          .withReconnectBackoff(10.seconds)
          .properties(AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG)
          .contains("10000")
      }
    }

    it("should provide withReconnectBackoffMax") {
      assert {
        settings
          .withReconnectBackoffMax(10.seconds)
          .properties(AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG)
          .contains("10000")
      }
    }

    it("should provide withRetryBackoff") {
      assert {
        settings
          .withRetryBackoff(10.seconds)
          .properties(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG)
          .contains("10000")
      }
    }

    it("should provide withConnectionsMaxIdle") {
      assert {
        settings
          .withConnectionsMaxIdle(10.seconds)
          .properties(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG)
          .contains("10000")
      }
    }

    it("should provide withRequestTimeout") {
      assert {
        settings
          .withRequestTimeout(10.seconds)
          .properties(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG)
          .contains("10000")
      }
    }

    it("should provide withMetadataMaxAge") {
      assert {
        settings
          .withMetadataMaxAge(10.seconds)
          .properties(AdminClientConfig.METADATA_MAX_AGE_CONFIG)
          .contains("10000")
      }
    }

    it("should provide withRetries") {
      assert {
        settings
          .withRetries(10)
          .properties(AdminClientConfig.RETRIES_CONFIG)
          .contains("10")
      }
    }

    it("should provide withProperty") {
      assert {
        settings
          .withProperty("a", "b")
          .properties("a")
          .contains("b")
      }
    }

    it("should provide withProperties") {
      assert {
        settings
          .withProperties("a" -> "b")
          .properties("a")
          .contains("b") &&
        settings
          .withProperties(Map("a" -> "b"))
          .properties("a")
          .contains("b")
      }
    }

    it("should provide withCloseTimeout") {
      assert {
        settings
          .withCloseTimeout(10.seconds)
          .closeTimeout == 10.seconds
      }
    }

    it("should provide withCreateAdminClient") {
      assert {
        settings
          .withCreateAdminClient(_ => IO.raiseError(new RuntimeException))
          .createAdminClient
          .attempt
          .unsafeRunSync()
          .isLeft
      }
    }

    it("should provide withBlocker") {
      assert {
        Resource.unit[IO]
          .use { blocker =>
            IO {
              settings
                .withBlocker(blocker)
                .blocker
                .isDefined
            }
          }
          .unsafeRunSync()
      }
    }

    it("should not provide a blocker unless set") {
      assert {
        settings.blocker.isEmpty
      }
    }

    it("should have a Show instance and matching toString") {
      assert {
        settings.toString == "AdminClientSettings(closeTimeout = 20 seconds)" &&
        settings.show == settings.toString
      }
    }
  }

  val settings =
    AdminClientSettings[IO]
}

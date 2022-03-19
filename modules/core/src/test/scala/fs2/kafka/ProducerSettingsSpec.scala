package fs2.kafka

import cats.syntax.all._
import org.apache.kafka.clients.producer.ProducerConfig

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

final class ProducerSettingsSpec extends BaseSpec {
  describe("ProducerSettings.default") {

    it("should provide withBootstrapServers") {
      assert {
        ProducerSettings.default
          .withBootstrapServers("localhost")
          .properties(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)
          .contains("localhost")
      }
    }

    it("should provide withAcks") {
      assert {
        ProducerSettings.default
          .withAcks(Acks.All)
          .properties(ProducerConfig.ACKS_CONFIG)
          .contains("all") &&
        ProducerSettings.default
          .withAcks(Acks.One)
          .properties(ProducerConfig.ACKS_CONFIG)
          .contains("1") &&
        ProducerSettings.default
          .withAcks(Acks.Zero)
          .properties(ProducerConfig.ACKS_CONFIG)
          .contains("0")
      }
    }

    it("should provide withBatchSize") {
      assert {
        ProducerSettings.default
          .withBatchSize(10)
          .properties(ProducerConfig.BATCH_SIZE_CONFIG)
          .contains("10")
      }
    }

    it("should provide withClientId") {
      assert {
        ProducerSettings.default
          .withClientId("clientId")
          .properties(ProducerConfig.CLIENT_ID_CONFIG)
          .contains("clientId")
      }
    }

    it("should provide withRetries") {
      assert {
        ProducerSettings.default
          .withRetries(10)
          .properties(ProducerConfig.RETRIES_CONFIG)
          .contains("10")
      }
    }

    it("should provide withMaxInFlightRequestsPerConnection") {
      assert {
        ProducerSettings.default
          .withMaxInFlightRequestsPerConnection(10)
          .properties(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION)
          .contains("10")
      }
    }

    it("should provide withEnableIdempotence") {
      assert {
        ProducerSettings.default
          .withEnableIdempotence(true)
          .properties(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)
          .contains("true") &&
        ProducerSettings.default
          .withEnableIdempotence(false)
          .properties(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)
          .contains("false")
      }
    }

    it("should provide withLinger") {
      assert {
        ProducerSettings.default
          .withLinger(10.seconds)
          .properties(ProducerConfig.LINGER_MS_CONFIG)
          .contains("10000")
      }
    }

    it("should provide withRequestTimeout") {
      assert {
        ProducerSettings.default
          .withRequestTimeout(10.seconds)
          .properties(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG)
          .contains("10000")
      }
    }

    it("should provide withDeliveryTimeout") {
      assert {
        ProducerSettings.default
          .withDeliveryTimeout(10.seconds)
          .properties(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG)
          .contains("10000")
      }
    }

    it("should provide withProperty/withProperties") {
      assert {
        ProducerSettings.default.withProperty("a", "b").properties("a").contains("b") &&
        ProducerSettings.default.withProperties("a" -> "b").properties("a").contains("b") &&
        ProducerSettings.default.withProperties(Map("a" -> "b")).properties("a").contains("b")
      }
    }

    it("should provide withCloseTimeout") {
      assert(ProducerSettings.default.withCloseTimeout(30.seconds).closeTimeout == 30.seconds)
    }

    it("should have a Show instance and matching toString") {
      val shown = ProducerSettings.default.withProperty("foo", "bar").show

      assert(
        shown == "ProducerSettings.default(closeTimeout = 60 seconds)" &&
          shown == ProducerSettings.default.toString
      )
    }

    it("should be able to set a custom blocking context") {
      assert {
        ProducerSettings.default.customBlockingContext.isEmpty &&
        ProducerSettings.default
          .withCustomBlockingContext(ExecutionContext.global)
          .customBlockingContext === Some(
          ExecutionContext.global
        )
      }
    }

  }
}

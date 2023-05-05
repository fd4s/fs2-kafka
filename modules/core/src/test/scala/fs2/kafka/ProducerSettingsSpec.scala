/*
 * Copyright 2018-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all._
import org.apache.kafka.clients.producer.ProducerConfig

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

final class ProducerSettingsSpec extends BaseSpec {
  describe("ProducerSettings") {
    it("should provide apply") {
      ProducerSettings[IO, String, String]
      ProducerSettings(Serializer[IO, String], Serializer[IO, String])
    }

    it("should provide withBootstrapServers") {
      assert {
        settings
          .withBootstrapServers("localhost")
          .properties(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)
          .contains("localhost")
      }
    }

    it("should provide withAcks") {
      assert {
        settings
          .withAcks(Acks.All)
          .properties(ProducerConfig.ACKS_CONFIG)
          .contains("all") &&
        settings
          .withAcks(Acks.One)
          .properties(ProducerConfig.ACKS_CONFIG)
          .contains("1") &&
        settings
          .withAcks(Acks.Zero)
          .properties(ProducerConfig.ACKS_CONFIG)
          .contains("0")
      }
    }

    it("should provide withBatchSize") {
      assert {
        settings
          .withBatchSize(10)
          .properties(ProducerConfig.BATCH_SIZE_CONFIG)
          .contains("10")
      }
    }

    it("should provide withClientId") {
      assert {
        settings
          .withClientId("clientId")
          .properties(ProducerConfig.CLIENT_ID_CONFIG)
          .contains("clientId")
      }
    }

    it("should provide withRetries") {
      assert {
        settings
          .withRetries(10)
          .properties(ProducerConfig.RETRIES_CONFIG)
          .contains("10")
      }
    }

    it("should provide withMaxInFlightRequestsPerConnection") {
      assert {
        settings
          .withMaxInFlightRequestsPerConnection(10)
          .properties(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION)
          .contains("10")
      }
    }

    it("should provide withEnableIdempotence") {
      assert {
        settings
          .withEnableIdempotence(true)
          .properties(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)
          .contains("true") &&
        settings
          .withEnableIdempotence(false)
          .properties(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)
          .contains("false")
      }
    }

    it("should provide withLinger") {
      assert {
        settings
          .withLinger(10.seconds)
          .properties(ProducerConfig.LINGER_MS_CONFIG)
          .contains("10000")
      }
    }

    it("should provide withRequestTimeout") {
      assert {
        settings
          .withRequestTimeout(10.seconds)
          .properties(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG)
          .contains("10000")
      }
    }

    it("should provide withDeliveryTimeout") {
      assert {
        settings
          .withDeliveryTimeout(10.seconds)
          .properties(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG)
          .contains("10000")
      }
    }

    it("should provide withProperty/withProperties") {
      assert {
        settings.withProperty("a", "b").properties("a").contains("b") &&
        settings.withProperties("a" -> "b").properties("a").contains("b") &&
        settings.withProperties(Map("a" -> "b")).properties("a").contains("b")
      }
    }

    it("should provide withCloseTimeout") {
      assert(settings.withCloseTimeout(30.seconds).closeTimeout == 30.seconds)
    }

    it("should have a Show instance and matching toString") {
      val shown = settings.show

      assert(
        shown == "ProducerSettings(closeTimeout = 60 seconds)" &&
          shown == settings.toString
      )
    }

    it("should be able to create with and without serializer creation effects") {
      val serializer = Serializer[IO, String]
      val recordSerializer = RecordSerializer.lift(serializer)

      ProducerSettings(serializer, serializer)
      ProducerSettings(recordSerializer, serializer)
      ProducerSettings(serializer, recordSerializer)
      ProducerSettings(recordSerializer, recordSerializer)
    }

    it("should be able to implicitly create with and without serializer creation effects") {
      val serializerInstance =
        Serializer[IO, String]
          .mapBytes(identity)

      implicit val serializer: RecordSerializer[IO, String] =
        RecordSerializer.lift(serializerInstance)

      ProducerSettings[IO, Int, Int]
      ProducerSettings[IO, String, Int].keySerializer.unsafeRunSync() shouldBe serializerInstance
      ProducerSettings[IO, Int, String].valueSerializer.unsafeRunSync() shouldBe serializerInstance
      ProducerSettings[IO, String, String]
    }

    it("should be able to set a custom blocking context") {
      assert {
        settings.customBlockingContext.isEmpty &&
        settings.withCustomBlockingContext(ExecutionContext.global).customBlockingContext === Some(
          ExecutionContext.global
        )
      }
    }
  }

  val settings = ProducerSettings[IO, String, String]
}

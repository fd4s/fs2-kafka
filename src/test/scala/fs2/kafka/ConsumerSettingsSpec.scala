package fs2.kafka

import cats.effect.IO
import cats.implicits._
import org.apache.kafka.clients.consumer.ConsumerConfig

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

final class ConsumerSettingsSpec extends BaseSpec {
  describe("ConsumerSettings") {
    it("should be able to override execution context") {
      assert {
        settings.executionContext.isEmpty &&
        settingsWithContext.executionContext.nonEmpty
      }
    }

    it("should provide withBootstrapServers") {
      assert {
        settings
          .withBootstrapServers("localhost")
          .properties(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)
          .contains("localhost")
      }
    }

    it("should provide withAutoOffsetReset") {
      assert {
        settings
          .withAutoOffsetReset(AutoOffsetReset.Earliest)
          .properties(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)
          .contains("earliest") &&
        settings
          .withAutoOffsetReset(AutoOffsetReset.Latest)
          .properties(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)
          .contains("latest") &&
        settings
          .withAutoOffsetReset(AutoOffsetReset.None)
          .properties(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)
          .contains("none")
      }
    }

    it("should provide withClientId") {
      assert {
        settings
          .withClientId("client")
          .properties(ConsumerConfig.CLIENT_ID_CONFIG)
          .contains("client")
      }
    }

    it("should provide withGroupId") {
      assert {
        settings.groupId.isEmpty &&
        settings
          .withGroupId("group")
          .properties(ConsumerConfig.GROUP_ID_CONFIG)
          .contains("group") &&
        settings
          .withGroupId("group")
          .groupId
          .contains("group")
      }
    }

    it("should provide withMaxPollRecords") {
      assert {
        settings
          .withMaxPollRecords(10)
          .properties(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)
          .contains("10")
      }
    }

    it("should provide withMaxPollInterval") {
      assert {
        settings
          .withMaxPollInterval(10.seconds)
          .properties(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG)
          .contains("10000")
      }
    }

    it("should provide withSessionTimeout") {
      assert {
        settings
          .withSessionTimeout(10.seconds)
          .properties(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG)
          .contains("10000")
      }
    }

    it("should provide withHeartbeatInterval") {
      assert {
        settings
          .withHeartbeatInterval(10.seconds)
          .properties(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG)
          .contains("10000")
      }
    }

    it("should provide withEnableAutoCommit") {
      assert {
        settings
          .withEnableAutoCommit(true)
          .properties(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)
          .contains("true") &&
        settings
          .withEnableAutoCommit(false)
          .properties(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)
          .contains("false")
      }
    }

    it("should provide withAutoCommitInterval") {
      assert {
        settings
          .withAutoCommitInterval(10.seconds)
          .properties(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG)
          .contains("10000")
      }
    }

    it("should provide withRequestTimeout") {
      assert {
        settings
          .withRequestTimeout(10.seconds)
          .properties(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG)
          .contains("10000")
      }
    }

    it("should provide withDefaultApiTimeout") {
      assert {
        settings
          .withDefaultApiTimeout(10.seconds)
          .properties(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG)
          .contains("10000")
      }
    }

    it("should provide withIsolationLevel") {
      assert {
        settings
          .withIsolationLevel(IsolationLevel.ReadCommitted)
          .properties(ConsumerConfig.ISOLATION_LEVEL_CONFIG)
          .contains("read_committed")
        settings
          .withIsolationLevel(IsolationLevel.ReadUncommitted)
          .properties(ConsumerConfig.ISOLATION_LEVEL_CONFIG)
          .contains("read_uncommitted")
      }
    }

    it("should provide withProperty") {
      assert {
        settings.withProperty("a", "b").properties("a").contains("b")
      }
    }

    it("should provide withProperties") {
      assert {
        settings.withProperties("a" -> "b").properties("a").contains("b") &&
        settings.withProperties(Map("a" -> "b")).properties("a").contains("b")
      }
    }

    it("should provide withCloseTimeout") {
      assert {
        settings
          .withCloseTimeout(50.millis)
          .closeTimeout == 50.millis
      }
    }

    it("should provide withCommitTimeout") {
      assert {
        settings
          .withCommitTimeout(50.millis)
          .commitTimeout == 50.millis
      }
    }

    it("should provide withPollInterval") {
      assert {
        settings
          .withPollInterval(50.millis)
          .pollInterval == 50.millis
      }
    }

    it("should provide withPollTimeout") {
      assert {
        settings
          .withPollTimeout(50.millis)
          .pollTimeout == 50.millis
      }
    }

    it("should provide withCommitRecovery") {
      assert {
        settings
          .withCommitRecovery(CommitRecovery.Default)
          .commitRecovery == CommitRecovery.Default
      }
    }

    it("should provide withCreateConsumer") {
      assert {
        settings
          .withCreateConsumer(_ => IO.raiseError(new RuntimeException))
          .createConsumer
          .attempt
          .unsafeRunSync
          .isLeft
      }
    }

    it("should provide withRecordMetadata") {
      val record = ConsumerRecord("topic", 0, 0L, "key", "value")

      assert {
        settings.recordMetadata(record) == "" &&
        settings
          .withRecordMetadata(_ => "ok")
          .recordMetadata(record) == "ok"
      }
    }

    it("should provide withMaxPrefetchBatches") {
      assert {
        settings
          .withMaxPrefetchBatches(3)
          .maxPrefetchBatches == 3
      }

      assert {
        settings
          .withMaxPrefetchBatches(2)
          .maxPrefetchBatches == 2
      }

      assert {
        settings
          .withMaxPrefetchBatches(1)
          .maxPrefetchBatches == 2
      }
    }

    it("should provide withShiftDeserialization") {
      assert {
        settings
          .withShiftDeserialization(false)
          .shiftDeserialization == false
      }
    }

    it("should have a Show instance and matching toString") {
      assert {
        settings.show == "ConsumerSettings(closeTimeout = 20 seconds, commitTimeout = 15 seconds, pollInterval = 50 milliseconds, pollTimeout = 50 milliseconds, commitRecovery = Default, shiftDeserialization = true)" &&
        settings.show == settings.toString
      }
    }
  }

  val settings =
    ConsumerSettings(
      keyDeserializer = Deserializer[IO, String],
      valueDeserializer = Deserializer[IO, String]
    )

  val settingsWithContext =
    ConsumerSettings[IO, String, String]
      .withExecutionContext(ExecutionContext.global)
}

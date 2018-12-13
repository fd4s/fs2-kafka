package fs2.kafka

import cats.effect.IO
import cats.implicits._
import fs2.Stream
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException
import org.apache.kafka.common.TopicPartition
import scala.concurrent.duration._

final class KafkaConsumerSpec extends BaseKafkaSpec {
  describe("KafkaConsumer#stream") {
    tests(_.stream)
  }

  describe("KafkaConsumer#partitionedStream") {
    tests(_.partitionedStream.parJoinUnbounded)
  }

  type Consumer = KafkaConsumer[IO, String, String]

  type ConsumerStream = Stream[IO, CommittableMessage[IO, String, String]]

  def tests(stream: Consumer => ConsumerStream): Unit = {
    it("should consume all messages") {
      withKafka { (config, topic) =>
        createCustomTopic(topic, partitions = 3)
        val produced = (0 until 100).map(n => s"key-$n" -> s"value->$n")
        publishToKafka(topic, produced)

        val consumed =
          consumerStream[IO]
            .using(consumerSettings(config))
            .evalTap(_.subscribeTo(topic))
            .flatMap(stream)
            .take(produced.size.toLong)
            .map(message => message.record.key -> message.record.value)
            .compile
            .toVector
            .unsafeRunSync

        consumed should contain theSameElementsAs produced
      }
    }

    it("should commit the last processed offsets") {
      withKafka { (config, topic) =>
        createCustomTopic(topic, partitions = 3)
        val produced = (0 until 100).map(n => s"key-$n" -> s"value->$n")
        publishToKafka(topic, produced)

        val committed =
          consumerStream[IO]
            .using(consumerSettings(config))
            .evalTap(_.subscribe(topic.r))
            .flatMap { consumer =>
              stream(consumer)
                .take(produced.size.toLong)
                .map(_.committableOffset)
                .fold(CommittableOffsetBatch.empty[IO])(_ updated _)
                .evalMap(batch => batch.commit.as(batch.offsets))
            }
            .compile
            .lastOrError
            .unsafeRunSync

        assert {
          committed.values.toList.foldMap(_.offset) == produced.size.toLong &&
          withKafkaConsumer[String, String](consumerProperties(config)) { consumer =>
            committed.foldLeft(true) {
              case (result, (topicPartition, offsetAndMetadata)) =>
                result && offsetAndMetadata == consumer.committed(topicPartition)
            }
          }
        }
      }
    }

    it("should interrupt the stream when cancelled") {
      withKafka { (config, topic) =>
        val consumed =
          consumerStream[IO]
            .using(consumerSettings(config))
            .evalTap(_.subscribeTo(topic))
            .evalTap(_.fiber.cancel)
            .flatTap(stream)
            .evalTap(_.fiber.join)
            .compile
            .toVector
            .unsafeRunSync

        assert(consumed.isEmpty)
      }
    }

    it("should fail with an error if not subscribed") {
      withKafka { (config, topic) =>
        createCustomTopic(topic, partitions = 3)

        val consumed =
          consumerStream[IO]
            .using(consumerSettings(config))
            .flatMap(stream)
            .compile
            .lastOrError
            .attempt
            .unsafeRunSync

        assert(consumed.left.toOption.map(_.toString).contains(NotSubscribedException().toString))
      }
    }

    it("should propagate consumer errors to stream") {
      withKafka { (config, topic) =>
        createCustomTopic(topic, partitions = 3)

        val consumed =
          consumerStream[IO]
            .using {
              consumerSettings(config)
                .withAutoOffsetReset(AutoOffsetReset.None)
            }
            .evalTap(_.subscribeTo(topic))
            .flatMap(stream)
            .compile
            .lastOrError
            .attempt
            .unsafeRunSync

        consumed.left.toOption match {
          case Some(_: NoOffsetForPartitionException) => succeed
          case Some(cause)                            => fail("Unexpected exception", cause)
          case None                                   => fail(s"Unexpected result [$consumed]")
        }
      }
    }

    it("should be able to retrieve offsets") {
      withKafka { (config, topic) =>
        createCustomTopic(topic, partitions = 1)
        val produced = (0 until 100).map(n => s"key-$n" -> s"value->$n")
        publishToKafka(topic, produced)

        val topicPartition = new TopicPartition(topic, 0)
        val topicPartitions = Set(topicPartition)
        val timeout = 10.seconds

        consumerStream[IO]
          .using(consumerSettings(config))
          .evalTap(_.subscribeTo(topic))
          .flatTap { consumer =>
            stream(consumer)
              .take(produced.size.toLong)
              .map(_.committableOffset)
              .to(commitBatch)
          }
          .evalTap { consumer =>
            for {
              start <- consumer.beginningOffsets(topicPartitions)
              startTimeout <- consumer.beginningOffsets(topicPartitions, timeout)
              _ <- IO(assert(start == startTimeout && start == Map(topicPartition -> 0L)))
            } yield ()
          }
          .evalTap { consumer =>
            for {
              end <- consumer.endOffsets(topicPartitions)
              endTimeout <- consumer.endOffsets(topicPartitions, timeout)
              _ <- IO(
                assert(end == endTimeout && end == Map(topicPartition -> produced.size.toLong)))
            } yield ()
          }
          .compile
          .drain
          .unsafeRunSync
      }
    }
  }
}

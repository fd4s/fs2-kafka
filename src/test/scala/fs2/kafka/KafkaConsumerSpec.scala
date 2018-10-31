package fs2.kafka

import cats.data.NonEmptyList
import cats.effect.IO
import cats.implicits._
import fs2.Stream

final class KafkaConsumerSpec extends BaseKafkaSpec {
  it("should consume all messages") {
    withKafka { (config, topic) =>
      createCustomTopic(topic, partitions = 3)
      val produced = (0 until 1000).map(n => s"key-$n" -> s"value->$n")
      publishToKafka(topic, produced)

      val consumed =
        (for {
          consumerSettings <- consumerSettings(config)
          consumer <- consumerStream[IO].using(consumerSettings)
          _ <- consumer.subscribe(NonEmptyList.of(topic))
          consumed <- consumer.stream.take(produced.size.toLong)
          record = consumed.record
        } yield record.key -> record.value).compile.toVector.unsafeRunSync

      consumed should contain theSameElementsAs produced
    }
  }

  it("should commit the last processed offsets") {
    withKafka { (config, topic) =>
      createCustomTopic(topic, partitions = 3)
      val produced = (0 until 100).map(n => s"key-$n" -> s"value->$n")
      publishToKafka(topic, produced)

      val committed =
        (for {
          consumerSettings <- consumerSettings(config)
          consumer <- consumerStream[IO].using(consumerSettings)
          _ <- consumer.subscribe(NonEmptyList.of(topic))
          offsets <- consumer.stream
            .take(produced.size.toLong)
            .map(_.committableOffset)
            .fold(CommittableOffsetBatch.empty[IO])(_ updated _)
            .evalMap(batch => batch.commit.as(batch.offsets))
        } yield offsets).compile.lastOrError.unsafeRunSync

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
        (for {
          consumerSettings <- consumerSettings(config)
          consumer <- consumerStream[IO].using(consumerSettings)
          _ <- consumer.subscribe(NonEmptyList.of(topic))
          _ <- Stream.eval(consumer.fiber.cancel)
          _ <- consumer.stream
          _ <- Stream.eval(consumer.fiber.join)
        } yield ()).compile.toVector.unsafeRunSync

      assert(consumed.isEmpty)
    }
  }

  it("should fail with an error if not subscribed") {
    withKafka { (config, topic) =>
      createCustomTopic(topic, partitions = 3)

      val consumed =
        (for {
          consumerSettings <- consumerSettings(config)
          consumer <- consumerStream[IO].using(consumerSettings)
          _ <- consumer.stream
        } yield ()).compile.lastOrError.attempt.unsafeRunSync

      assert(consumed.left.toOption.contains(NotSubscribedException))
    }
  }
}

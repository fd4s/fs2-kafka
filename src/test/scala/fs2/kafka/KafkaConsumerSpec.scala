package fs2.kafka

import cats.effect.IO
import cats.effect.concurrent.Ref
import cats.implicits._
import fs2.Stream
import fs2.concurrent.Queue
import net.manub.embeddedkafka.EmbeddedKafkaConfig
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
            .evalTap(consumer => IO(consumer.toString should startWith("KafkaConsumer$")).void)
            .flatMap(stream)
            .take(produced.size.toLong)
            .map(message => message.record.key -> message.record.value)
            .compile
            .toVector
            .unsafeRunSync

        consumed should contain theSameElementsAs produced
      }
    }

    it("should handle rebalance") {
      withKafka { (config, topic) =>
        createCustomTopic(topic, partitions = 3)
        val produced1 = (0 until 100).map(n => s"key-$n" -> s"value->$n")
        val produced2 = (100 until 200).map(n => s"key-$n" -> s"value->$n")
        val producedTotal = produced1.size.toLong + produced2.size.toLong

        val consumer = (queue: Queue[IO, CommittableMessage[IO, String, String]]) =>
          consumerStream[IO]
            .using(consumerSettings(config))
            .evalTap(_.subscribeTo(topic))
            .evalMap(stream(_).evalMap(queue.enqueue1).compile.drain.start.void)

        (for {
          queue <- Stream.eval(Queue.unbounded[IO, CommittableMessage[IO, String, String]])
          ref <- Stream.eval(Ref.of[IO, Set[String]](Set.empty))
          _ <- consumer(queue)
          _ <- Stream.eval(IO.sleep(5.seconds))
          _ <- Stream.eval(IO(publishToKafka(topic, produced1)))
          _ <- consumer(queue)
          _ <- Stream.eval(IO.sleep(5.seconds))
          _ <- Stream.eval(IO(publishToKafka(topic, produced2)))
          _ <- Stream.eval {
            queue.dequeue
              .evalMap { message =>
                ref.modify { keys =>
                  val newKeys = keys + message.record.key
                  (newKeys, newKeys)
                }
              }
              .takeWhile(_.size < 200)
              .compile
              .drain
          }
          keys <- Stream.eval(ref.get)
          _ <- Stream.eval(IO(assert {
            keys.size.toLong == producedTotal && {
              keys == (0 until 200).map(n => s"key-$n").toSet
            }
          }))
        } yield ()).compile.drain.unsafeRunSync
      }
    }

    it("should read from the given offset") {
      withKafka {
        seekTest(numMessages = 100, readOffset = 90)
      }
    }

    it("should fail to read from a negative offset") {
      withKafka {
        an[IllegalArgumentException] should be thrownBy seekTest(numMessages = 100,
                                                                 readOffset = -1)(_, _)
      }
    }

    it("should fail to read from a partition not assigned to this consumer") {
      withKafka {
        an[IllegalStateException] should be thrownBy seekTest(numMessages = 100,
                                                              readOffset = 90,
                                                              partition = Some(123))(_, _)
      }
    }

    it("should commit the last processed offsets") {
      withKafka { (config, topic) =>
        createCustomTopic(topic, partitions = 3)
        val produced = (0 until 100).map(n => s"key-$n" -> s"value->$n")
        publishToKafka(topic, produced)

        val committed =
          consumerSettingsExecutionContext(config)
            .flatMap(consumerStream[IO].using)
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

    it("should fail with an error if subscribe is invalid") {
      withKafka { (config, _) =>
        val subscribeName =
          consumerStream[IO]
            .using(consumerSettings(config))
            .evalMap(_.subscribeTo(""))
            .compile
            .lastOrError
            .attempt
            .unsafeRunSync

        assert {
          subscribeName.left.toOption
            .map(_.toString)
            .contains {
              "java.lang.IllegalArgumentException: Topic collection to subscribe to cannot contain null or empty topic"
            }
        }

        val subscribeRegex =
          consumerStream[IO]
            .using(consumerSettings(config))
            .evalTap(_.subscribeTo("topic"))
            .evalMap(_.subscribe("".r))
            .compile
            .lastOrError
            .attempt
            .unsafeRunSync

        assert {
          subscribeRegex.left.toOption
            .map(_.toString)
            .contains {
              "java.lang.IllegalStateException: Subscription to topics, partitions and pattern are mutually exclusive"
            }
        }
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
              .through(commitBatch)
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

    def seekTest(numMessages: Long, readOffset: Long, partition: Option[Int] = None)(
      config: EmbeddedKafkaConfig,
      topic: String) = {
      createCustomTopic(topic)

      val produced = (0l until numMessages).map(n => s"key-$n" -> s"value->$n")
      publishToKafka(topic, produced)

      val consumed =
        consumerStream[IO]
          .using(consumerSettings(config))
          .flatMap { consumer =>
            val validSeekParams =
              stream(consumer)
                .take(Math.max(readOffset, 1))
                .map(_.committableOffset)
                .compile
                .toList
                .map(_.last)
                .map(co => (co.topicPartition, co.offsetAndMetadata.offset()))

            val seekParams =
              validSeekParams.map {
                case (topicPartition, offset) =>
                  val p = partition.map(new TopicPartition(topic, _)).getOrElse(topicPartition)
                  val o = Math.min(readOffset, offset)

                  (p, o)
              }

            val setOffset =
              seekParams.flatMap { case (tp, o) => consumer.seek(tp, o) }

            val consume = stream(consumer).take(numMessages - readOffset)

            Stream.eval(consumer.subscribeTo(topic)).drain ++
              (Stream.eval_(setOffset) ++ consume)
                .map(_.record)
                .map(record => record.key -> record.value)
          }
          .compile
          .toVector
          .unsafeRunSync()

      consumed should contain theSameElementsAs produced.drop(readOffset.toInt)
    }
  }
}

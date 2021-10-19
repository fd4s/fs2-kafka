package fs2.kafka

import java.util

import cats.data.NonEmptyList
import cats.effect.IO
import cats.implicits._
import fs2.{Chunk, Stream}
import fs2.kafka.internal.converters.collection._
import org.apache.kafka.clients.consumer.{ConsumerConfig, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.InvalidProducerEpochException
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.scalatest.EitherValues

import scala.concurrent.duration._

class TransactionalKafkaProducerSpec extends BaseKafkaSpec with EitherValues {
  describe("creating transactional producers") {
    it("should support defined syntax") {
      val settings = TransactionalProducerSettings("id", ProducerSettings[IO, String, String])

      TransactionalKafkaProducer.resource[IO, String, String](settings)
      TransactionalKafkaProducer[IO].resource(settings)

      TransactionalKafkaProducer.stream[IO, String, String](settings)
      TransactionalKafkaProducer[IO].resource(settings)

      TransactionalKafkaProducer[IO].toString should startWith(
        "TransactionalProducerPartiallyApplied$"
      )
    }
  }

  it("should be able to produce single records in a transaction") {
    withTopic { topic =>
      createCustomTopic(topic, partitions = 3)
      val toProduce = (0 to 10).map(n => s"key-$n" -> s"value-$n")

      val produced =
        (for {
          producer <- TransactionalKafkaProducer.stream(
            TransactionalProducerSettings(
              "id",
              producerSettings[IO]
                .withRetries(Int.MaxValue)
            )
          )
          _ <- Stream.eval(IO(producer.toString should startWith("TransactionalKafkaProducer$")))
          records <- Stream.chunk(Chunk.seq(toProduce)).zipWithIndex.map {
            case ((key, value), i) =>
              val offset =
                CommittableOffset[IO](
                  new TopicPartition(topic, (i % 3).toInt),
                  new OffsetAndMetadata(i),
                  Some("group"),
                  _ => IO.unit
                )

              TransactionalProducerRecords.one(
                CommittableProducerRecords.one(
                  ProducerRecord(topic, key, value),
                  offset
                ),
                (key, value)
              )
          }
          passthrough <- Stream
            .eval(producer.produce(records))
            .map(_.passthrough)
            .buffer(toProduce.size)
        } yield passthrough).compile.toVector.unsafeRunSync()

      produced should contain theSameElementsAs toProduce

      val consumed = {
        consumeNumberKeyedMessagesFrom[String, String](
          topic,
          produced.size,
          customProperties = Map(ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed")
        )
      }

      consumed should contain theSameElementsAs produced.toList
    }
  }

  it("should be able to produce multiple records in a transaction") {
    withTopic { topic =>
      createCustomTopic(topic, partitions = 3)
      val toProduce =
        Chunk.seq((0 to 100).toList.map(n => s"key-$n" -> s"value-$n"))

      val toPassthrough = "passthrough"

      val produced =
        (for {
          producer <- TransactionalKafkaProducer.stream(
            TransactionalProducerSettings(
              "id",
              producerSettings[IO]
                .withRetries(Int.MaxValue)
            )
          )
          recordsToProduce = toProduce.map {
            case (key, value) => ProducerRecord(topic, key, value)
          }
          offsets = toProduce.mapWithIndex {
            case (_, i) =>
              CommittableOffset[IO](
                new TopicPartition(topic, i % 3),
                new OffsetAndMetadata(i.toLong),
                Some("group"),
                _ => IO.unit
              )
          }
          records = TransactionalProducerRecords(
            recordsToProduce.zip(offsets).map {
              case (record, offset) =>
                CommittableProducerRecords.one(
                  record,
                  offset
                )
            },
            toPassthrough
          )
          result <- Stream.eval(producer.produce(records))
        } yield result).compile.lastOrError.unsafeRunSync()

      val records =
        produced.records.map {
          case (record, _) =>
            record.key -> record.value
        }

      assert(records == toProduce && produced.passthrough == toPassthrough)

      val consumed = {
        val customConsumerProperties =
          Map(ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed")
        consumeNumberKeyedMessagesFrom[String, String](
          topic,
          records.size,
          customProperties = customConsumerProperties
        )
      }

      consumed should contain theSameElementsAs records.toList
    }
  }

  it("should abort transactions if committing offsets fails") {
    withTopic { topic =>
      createCustomTopic(topic, partitions = 3)
      val toProduce = (0 to 100).toList.map(n => s"key-$n" -> s"value-$n").toList
      val toPassthrough = "passthrough"

      val error = new RuntimeException("BOOM")

      val produced =
        (for {
          producer <- TransactionalKafkaProducer.stream(
            TransactionalProducerSettings(
              "id",
              producerSettings[IO]
                .withRetries(Int.MaxValue)
                .withCreateProducer { properties =>
                  IO.delay {
                    new org.apache.kafka.clients.producer.KafkaProducer[Array[Byte], Array[Byte]](
                      (properties: Map[String, AnyRef]).asJava,
                      new ByteArraySerializer,
                      new ByteArraySerializer
                    ) {
                      override def sendOffsetsToTransaction(
                        offsets: util.Map[TopicPartition, OffsetAndMetadata],
                        consumerGroupId: String
                      ): Unit =
                        if (offsets.containsKey(new TopicPartition(topic, 2))) {
                          throw error
                        } else {
                          super.sendOffsetsToTransaction(offsets, consumerGroupId)
                        }
                    }
                  }
                }
            )
          )
          recordsToProduce = toProduce.map {
            case (key, value) => ProducerRecord(topic, key, value)
          }
          offsets = toProduce.mapWithIndex {
            case (_, i) =>
              CommittableOffset(
                new TopicPartition(topic, i % 3),
                new OffsetAndMetadata(i.toLong),
                Some("group"),
                _ => IO.unit
              )
          }
          records = TransactionalProducerRecords(
            Chunk.seq(recordsToProduce.zip(offsets)).map {
              case (record, offset) =>
                CommittableProducerRecords(
                  NonEmptyList.one(record),
                  offset
                )
            },
            toPassthrough
          )
          result <- Stream.eval(producer.produce(records).attempt)
        } yield result).compile.lastOrError.unsafeRunSync()

      produced shouldBe Left(error)

      val consumedOrError = {
        Either.catchNonFatal(
          consumeFirstKeyedMessageFrom[String, String](
            topic,
            customProperties = Map(ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed")
          )
        )
      }

      consumedOrError.isLeft shouldBe true
    }
  }

  it("should use user-specified transaction timeouts") {
    withTopic { topic =>
      createCustomTopic(topic, partitions = 3)
      val toProduce = (0 to 100).toList.map(n => s"key-$n" -> s"value-$n")

      val produced =
        (for {
          producer <- TransactionalKafkaProducer.stream(
            TransactionalProducerSettings(
              "id",
              producerSettings[IO]
                .withRetries(Int.MaxValue)
                .withCreateProducer { properties =>
                  IO.delay {
                    new org.apache.kafka.clients.producer.KafkaProducer[Array[Byte], Array[Byte]](
                      (properties: Map[String, AnyRef]).asJava,
                      new ByteArraySerializer,
                      new ByteArraySerializer
                    ) {
                      override def commitTransaction(): Unit = {
                        Thread.sleep(2 * transactionTimeoutInterval.toMillis)
                        super.commitTransaction()
                      }
                    }
                  }
                }
            ).withTransactionTimeout(transactionTimeoutInterval - 250.millis)
          )
          recordsToProduce = toProduce.map {
            case (key, value) => ProducerRecord(topic, key, value)
          }
          offset = CommittableOffset(
            new TopicPartition(topic, 1),
            new OffsetAndMetadata(recordsToProduce.length.toLong),
            Some("group"),
            _ => IO.unit
          )
          records = TransactionalProducerRecords.one(
            CommittableProducerRecords(recordsToProduce, offset)
          )
          result <- Stream.eval(producer.produce(records).attempt)
        } yield result).compile.lastOrError.unsafeRunSync()

      produced.left.value shouldBe an[InvalidProducerEpochException]

      val consumedOrError = {
        Either.catchNonFatal(
          consumeFirstKeyedMessageFrom[String, String](
            topic,
            customProperties = Map(ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed")
          )
        )
      }

      consumedOrError.isLeft shouldBe true
    }
  }

  it("should get metrics") {
    withTopic { topic =>
      createCustomTopic(topic, partitions = 3)

      val info =
        TransactionalKafkaProducer[IO]
          .stream(
            TransactionalProducerSettings(
              transactionalId = "id",
              producerSettings = producerSettings[IO].withRetries(Int.MaxValue)
            )
          )
          .evalMap(_.metrics)

      val res =
        info
          .take(1)
          .compile
          .lastOrError
          .unsafeRunSync()

      assert(res.nonEmpty)
    }
  }
}

package fs2.kafka

import java.util

import cats.data.NonEmptyList
import cats.effect.IO
import cats.implicits._
import fs2.{Chunk, Stream}
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.apache.kafka.clients.consumer.{ConsumerConfig, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.scalatest.OptionValues

import scala.collection.JavaConverters._

class TransactionalKafkaProducerSpec extends BaseKafkaSpec with OptionValues {

  it("should be able to produce single records in a transaction") {
    withKafka { (config, topic) =>
      createCustomTopic(topic, partitions = 3)
      val toProduce = (0 to 10).map(n => s"key-$n" -> s"value-$n")

      val produced =
        (for {
          producer <- transactionalProducerStream(
            producerSettings[IO](config)
              .withTransactionalId("transactions")
          )
          _ <- Stream.eval(IO(producer.toString should startWith("TransactionalKafkaProducer$")))
          message <- Stream.chunk(Chunk.seq(toProduce)).zipWithIndex.map {
            case ((key, value), i) =>
              val committableOffset =
                CommittableOffset[IO](
                  new TopicPartition(topic, (i % 3).toInt),
                  new OffsetAndMetadata(i),
                  Some("group"),
                  _ => IO.unit
                )

              TransactionalProducerMessage.one(
                CommittableProducerRecords.one(
                  ProducerRecord(topic, key, value),
                  committableOffset
                ),
                (key, value)
              )
          }
          passthrough <- Stream.eval(producer.producePassthrough(message)).buffer(toProduce.size)
        } yield passthrough).compile.toVector.unsafeRunSync()

      produced should contain theSameElementsAs toProduce

      val consumed = {
        implicit val transactionalConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
          kafkaPort = config.kafkaPort,
          zooKeeperPort = config.zooKeeperPort,
          customConsumerProperties = Map(ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed")
        )
        consumeNumberKeyedMessagesFrom[String, String](topic, produced.size)
      }

      consumed should contain theSameElementsAs produced.toList
    }
  }

  it("should be able to produce multiple records in a transaction") {
    withKafka { (config, topic) =>
      createCustomTopic(topic, partitions = 3)
      val toProduce =
        (0 to 100).toList.map(n => s"key-$n" -> s"value-$n").toList

      val toPassthrough = "passthrough"

      val produced =
        (for {
          producer <- transactionalProducerStream[IO, String, String](
            producerSettings[IO](config).withTransactionalId("transactions")
          )
          records = toProduce.map {
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
          message = TransactionalProducerMessage(
            Chunk.seq(records.zip(offsets)).map {
              case (record, offset) =>
                CommittableProducerRecords.one(
                  record,
                  offset
                )
            },
            toPassthrough
          )
          result <- Stream.eval(producer.produce(message))
        } yield result).compile.lastOrError.unsafeRunSync()

      val records =
        produced.records.map {
          case (record, _) =>
            record.key -> record.value
        }

      assert(records == toProduce && produced.passthrough == toPassthrough)

      val consumed = {
        implicit val transactionalConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
          kafkaPort = config.kafkaPort,
          zooKeeperPort = config.zooKeeperPort,
          customConsumerProperties = Map(ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed")
        )
        consumeNumberKeyedMessagesFrom[String, String](topic, records.size)
      }

      consumed should contain theSameElementsAs records.toList
    }
  }

  it("should abort transactions if committing offsets fails") {
    withKafka { (config, topic) =>
      createCustomTopic(topic, partitions = 3)
      val toProduce = (0 to 100).toList.map(n => s"key-$n" -> s"value-$n").toList
      val toPassthrough = "passthrough"

      val error = new RuntimeException("BOOM")

      val produced =
        (for {
          producer <- transactionalProducerStream[IO, String, String](
            producerSettings[IO](config)
              .withTransactionalId("transactions")
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
          records = toProduce.map {
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
          message = TransactionalProducerMessage(
            Chunk.seq(records.zip(offsets)).map {
              case (record, offset) =>
                CommittableProducerRecords(
                  NonEmptyList.one(record),
                  offset
                )
            },
            toPassthrough
          )
          result <- Stream.eval(producer.produce(message).attempt)
        } yield result).compile.lastOrError.unsafeRunSync()

      produced shouldBe Left(error)

      val consumedOrError = {
        implicit val transactionalConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
          kafkaPort = config.kafkaPort,
          zooKeeperPort = config.zooKeeperPort,
          customConsumerProperties = Map(ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed")
        )
        Either.catchNonFatal(consumeFirstKeyedMessageFrom[String, String](topic))
      }

      consumedOrError.isLeft shouldBe true
    }
  }
}

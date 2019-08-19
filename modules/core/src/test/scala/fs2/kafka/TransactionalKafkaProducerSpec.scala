package fs2.kafka

import java.util

import cats.data.NonEmptyList
import cats.effect.IO
import cats.implicits._
import fs2.{Chunk, Stream}
import fs2.kafka.internal.instances._
import fs2.kafka.internal.converters.collection._
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.apache.kafka.clients.consumer.{ConsumerConfig, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ProducerFencedException
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.scalatest.{EitherValues, OptionValues}

import scala.concurrent.duration._
import scala.collection.immutable.SortedSet

class TransactionalKafkaProducerSpec extends BaseKafkaSpec with OptionValues with EitherValues {

  private def builder(
    settings: ProducerSettings[IO, String, String],
    timeout: Option[FiniteDuration] = None
  ) = new TransactionalKafkaProducer.Builder(settings, timeout)

  describe("TransactionalKafkaProducer#withConstantId") {
    it("should be able to produce single records in a transaction") {
      withKafka { (config, topic) =>
        createCustomTopic(topic, partitions = 3)
        val toProduce = (0 to 10).map(n => s"key-$n" -> s"value-$n")

        val produced =
          (for {
            producer <- Stream.resource {
              builder(producerSettings[IO](config)).withConstantId("transactions")
            }
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
          implicit val transactionalConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
            kafkaPort = config.kafkaPort,
            zooKeeperPort = config.zooKeeperPort,
            customConsumerProperties =
              Map(ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed")
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
          Chunk.seq((0 to 100).toList.map(n => s"key-$n" -> s"value-$n"))

        val toPassthrough = "passthrough"

        val produced =
          (for {
            producer <- Stream.resource {
              builder(producerSettings[IO](config)).withConstantId("transactions")
            }
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
          implicit val transactionalConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
            kafkaPort = config.kafkaPort,
            zooKeeperPort = config.zooKeeperPort,
            customConsumerProperties =
              Map(ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed")
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
            producer <- Stream.resource {
              builder {
                producerSettings[IO](config)
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
              }.withConstantId("transactions")
            }
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
          implicit val transactionalConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
            kafkaPort = config.kafkaPort,
            zooKeeperPort = config.zooKeeperPort,
            customConsumerProperties =
              Map(ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed")
          )
          Either.catchNonFatal(consumeFirstKeyedMessageFrom[String, String](topic))
        }

        consumedOrError.isLeft shouldBe true
      }
    }

    it("should use user-specified transaction timeouts") {
      withKafka { (config, topic) =>
        createCustomTopic(topic, partitions = 3)
        val toProduce = (0 to 100).toList.map(n => s"key-$n" -> s"value-$n")

        val produced =
          (for {
            producer <- Stream.resource {
              builder(
                producerSettings[IO](config).withCreateProducer { properties =>
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
                },
                Some(transactionTimeoutInterval - 250.millis)
              ).withConstantId("transactions")
            }
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

        produced.left.value shouldBe a[ProducerFencedException]

        val consumedOrError = {
          implicit val transactionalConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
            kafkaPort = config.kafkaPort,
            zooKeeperPort = config.zooKeeperPort,
            customConsumerProperties =
              Map(ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed")
          )
          Either.catchNonFatal(consumeFirstKeyedMessageFrom[String, String](topic))
        }

        consumedOrError.isLeft shouldBe true
      }
    }
  }

  describe("TransactionalKafkaProducer#withRebalancingIds") {
    def noRebalance[F[_]](topic: String, numPartitions: Int): Stream[F, SortedSet[TopicPartition]] =
      Stream.emit(SortedSet((0 until numPartitions).map(new TopicPartition(topic, _)): _*))

    it("should be able to produce single records in a transaction") {
      withKafka { (config, topic) =>
        createCustomTopic(topic, partitions = 3)
        val toProduce = (0 to 10).map(n => s"key-$n" -> s"value-$n")

        val produced =
          (for {
            producer <- Stream.resource {
              builder(producerSettings[IO](config))
                .withDynamicIds("transactions", noRebalance[IO](topic, 3))
            }
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
          implicit val transactionalConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
            kafkaPort = config.kafkaPort,
            zooKeeperPort = config.zooKeeperPort,
            customConsumerProperties =
              Map(ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed")
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
          Chunk.seq((0 to 100).toList.map(n => s"key-$n" -> s"value-$n"))

        val toPassthrough = "passthrough"

        val produced =
          (for {
            producer <- Stream.resource {
              builder(producerSettings[IO](config))
                .withDynamicIds("transactions", noRebalance(topic, 3))
            }
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

        // NOTE: Since the rebalancing producer groups elements by topic/partition,
        // the order of output `ProducerResult`s doesn't necessarily match the order
        // of input `ProducerRecord`s.
        records.toList should contain theSameElementsAs toProduce.toList
        produced.passthrough shouldBe toPassthrough

        val consumed = {
          implicit val transactionalConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
            kafkaPort = config.kafkaPort,
            zooKeeperPort = config.zooKeeperPort,
            customConsumerProperties =
              Map(ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed")
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
            producer <- Stream.resource {
              builder {
                producerSettings[IO](config)
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
              }.withDynamicIds("transactions", noRebalance(topic, 3))
            }
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
          implicit val transactionalConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
            kafkaPort = config.kafkaPort,
            zooKeeperPort = config.zooKeeperPort,
            customConsumerProperties =
              Map(ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed")
          )
          Either.catchNonFatal(consumeFirstKeyedMessageFrom[String, String](topic))
        }

        // FIXME: This currently fails because committing offsets only fails in the producer
        // for one of the three topic/partitions. The other t/p's successfully commit messages,
        // but we don't currently have a mechanism to signal what worked and what didn't.
        consumedOrError.isLeft shouldBe true
      }
    }
  }
}

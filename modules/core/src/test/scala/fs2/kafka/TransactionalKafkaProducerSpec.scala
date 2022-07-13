package fs2.kafka

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all._
import fs2.kafka.internal.converters.collection._
import fs2.kafka.producer.MkProducer
import fs2.{Chunk, Stream}
import org.apache.kafka.clients.consumer.{ConsumerConfig, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
//import org.apache.kafka.common.errors.InvalidProducerEpochException
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.scalatest.EitherValues
import weaver.Expectations

import java.util.UUID
import scala.concurrent.duration._

//class TransactionalKafkaProducerSpec(g: GlobalRead) extends BaseWeaverSpec with EitherValues {
//
//  override def sharedResource: Resource[IO, KafkaContainer] = g.getOrFailR[KafkaContainer]()
object TransactionalKafkaProducerSpec extends BaseWeaverSpec with EitherValues {
  override lazy val container = BaseWeaverSpecShared.makeContainer()

  test("should support defined syntax") { _ =>
    val settings = TransactionalProducerSettings("id", ProducerSettings[IO, String, String])

    TransactionalKafkaProducer.resource[IO, String, String](settings)
    TransactionalKafkaProducer[IO].resource(settings)

    TransactionalKafkaProducer.stream[IO, String, String](settings)
    TransactionalKafkaProducer[IO].resource(settings)
    //
    IO(
      expect(
        TransactionalKafkaProducer[IO].toString.startsWith("TransactionalProducerPartiallyApplied$")
      )
    )
  }

  test("should be able to produce single records with offsets in a transaction") {
    withTopic { topic =>
      testSingle(
        topic,
        Some(
          i =>
            CommittableOffset[IO](
              new TopicPartition(topic, (i % 3).toInt),
              new OffsetAndMetadata(i),
              Some(topic),
              _ => IO.unit
            )
        )
      )
    }
  }

  test("should be able to produce single records without offsets in a transaction") {
    withTopic { topic =>
      testSingle(
        topic,
        None
      )
    }
  }

  private def testSingle(
    topic: String,
    makeOffset: Option[Long => CommittableOffset[IO]]
  ): IO[Expectations] =
    for {
      _ <- IO.blocking(createCustomTopic(topic, partitions = 3))
      toProduce = (0 to 10).map(n => s"key-$n" -> s"value-$n")

      produced <- (for {
        producer <- TransactionalKafkaProducer.stream(
          TransactionalProducerSettings(
            s"id-$topic",
            producerSettings[IO]
              .withRetries(Int.MaxValue)
          )
        )
        records <- Stream.chunk(Chunk.seq(toProduce)).zipWithIndex.map {
          case ((key, value), i) =>
            val record = ProducerRecord(topic, key, value)

            makeOffset.fold[
              Either[
                ProducerRecords[(String, String), String, String],
                TransactionalProducerRecords[IO, (String, String), String, String]
              ]
            ](Left(ProducerRecords.one(record, (key, value))))(
              offset =>
                Right(
                  TransactionalProducerRecords.one(
                    CommittableProducerRecords.one(
                      record,
                      offset(i)
                    ),
                    (key, value)
                  )
                )
            )

        }
        passthrough <- Stream
          .eval(records.fold(producer.produceWithoutOffsets, producer.produce))
          .map(_.passthrough)
          .buffer(toProduce.size)
      } yield passthrough).compile.toVector

      consumed <- IO.blocking {
        consumeNumberKeyedMessagesFrom[String, String](
          topic,
          produced.size,
          customProperties = Map(ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed")
        )
      }

    } yield expect(produced === toProduce.toVector) and expect(consumed.toSet === produced.toSet)

  test("should be able to produce multiple records with offsets in a transaction") {
    withTopic { topic =>
      testMultiple(
        topic,
        Some(
          i =>
            CommittableOffset[IO](
              new TopicPartition(topic, i % 3),
              new OffsetAndMetadata(i.toLong),
              Some(topic),
              _ => IO.unit
            )
        )
      )
    }
  }

  test("should be able to produce multiple records without offsets in a transaction") {
    withTopic { topic =>
      testMultiple(
        topic,
        None
      )
    }
  }

  private def testMultiple(
    topic: String,
    makeOffset: Option[Int => CommittableOffset[IO]]
  ): IO[Expectations] =
    for {
      _ <- IO.blocking(createCustomTopic(topic, partitions = 3))
      toProduce = Chunk.seq((0 to 100).toList.map(n => s"key-$n" -> s"value-$n"))

      toPassthrough = "passthrough"

      produced <- (for {
        producer <- TransactionalKafkaProducer.stream(
          TransactionalProducerSettings(
            s"id-$topic",
            producerSettings[IO]
              .withRetries(Int.MaxValue)
          )
        )
        recordsToProduce = toProduce.map {
          case (key, value) => ProducerRecord(topic, key, value)
        }

        produce = makeOffset match {
          case Some(offset) =>
            val offsets = toProduce.mapWithIndex {
              case (_, i) => offset(i)
            }
            val records = TransactionalProducerRecords(
              recordsToProduce.zip(offsets).map {
                case (record, offset) =>
                  CommittableProducerRecords.one(
                    record,
                    offset
                  )
              },
              toPassthrough
            )
            producer.produce(records)
          case None =>
            val records = ProducerRecords(recordsToProduce, toPassthrough)
            producer.produceWithoutOffsets(records)
        }

        result <- Stream.eval(produce)
      } yield result).compile.lastOrError

      records = produced.records.map {
        case (record, _) =>
          record.key -> record.value
      }

      consumed <- IO.blocking {
        val customConsumerProperties =
          Map(ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed")
        consumeNumberKeyedMessagesFrom[String, String](
          topic,
          records.size,
          customProperties = customConsumerProperties
        )
      }

    } yield expect(records === toProduce) and
      expect(produced.passthrough == toPassthrough) and
      expect(consumed.toSet === records.toList.toSet)

  test("should not allow concurrent access to a producer during a transaction") {
    IO {
      withTopic { topic =>
        createCustomTopic(topic, partitions = 3)
        val toProduce =
          Chunk.seq((0 to 1000000).toList.map(n => s"key-$n" -> s"value-$n"))

        val result =
          (for {
            producer <- TransactionalKafkaProducer.stream(
              TransactionalProducerSettings(
                s"id-$topic",
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
                  Some(topic),
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
              }
            )
            _ <- Stream
              .eval(producer.produce(records))
              .concurrently(
                Stream.eval(
                  producer.produce(
                    TransactionalProducerRecords.one(
                      CommittableProducerRecords.one(
                        ProducerRecord[String, String](topic, "test", "test"),
                        CommittableOffset[IO](
                          new TopicPartition(topic, 0),
                          new OffsetAndMetadata(0),
                          Some(topic),
                          _ => IO.unit
                        )
                      )
                    )
                  )
                )
              )
          } yield ()).compile.lastOrError.attempt.unsafeRunSync()

        assert(result == Right(()))
      }
    }
  }

  test("should abort transactions if committing offsets fails") {
    IO {
      withTopic { topic =>
        createCustomTopic(topic, partitions = 3)
        val toProduce = (0 to 100).toList.map(n => s"key-$n" -> s"value-$n").toList
        val toPassthrough = "passthrough"

        val error = new RuntimeException("BOOM")

        implicit val mk: MkProducer[IO] = new MkProducer[IO] {
          def apply[G[_]](settings: ProducerSettings[G, _, _]): IO[KafkaByteProducer] =
            IO.delay {
              new org.apache.kafka.clients.producer.KafkaProducer[Array[Byte], Array[Byte]](
                (settings.properties: Map[String, AnyRef]).asJava,
                new ByteArraySerializer,
                new ByteArraySerializer
              ) {
                override def sendOffsetsToTransaction(
                  offsets: java.util.Map[TopicPartition, OffsetAndMetadata],
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

        val produced =
          (for {
            producer <- TransactionalKafkaProducer.stream(
              TransactionalProducerSettings(
                s"id-$topic",
                producerSettings[IO]
                  .withRetries(Int.MaxValue)
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
                  Some(topic),
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

        expect(produced == Left(error))

        val consumedOrError = {
          Either.catchNonFatal(
            consumeFirstKeyedMessageFrom[String, String](
              topic,
              customProperties = Map(ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed")
            )
          )
        }

        expect(consumedOrError.isLeft)
      }
    }
  }

  test("should get metrics") {
    IO {
      withTopic { topic =>
        createCustomTopic(topic, partitions = 3)

        val info =
          TransactionalKafkaProducer[IO]
            .stream(
              TransactionalProducerSettings(
                transactionalId = s"id-$topic",
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

  test("should use user-specified transaction timeouts") {
    withTopic(3) { topic =>
      val toProduce = (0 to 100).toList.map(n => s"key-$n" -> s"value-$n")

      implicit val mkProducer: MkProducer[IO] = new MkProducer[IO] {
        def apply[G[_]](settings: ProducerSettings[G, _, _]): IO[KafkaByteProducer] = IO.delay {
          new org.apache.kafka.clients.producer.KafkaProducer[Array[Byte], Array[Byte]](
            (settings.properties: Map[String, AnyRef]).asJava,
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

      val recordsToProduce = toProduce.map {
        case (key, value) => ProducerRecord(topic, key, value)
      }
      val offset = CommittableOffset(
        new TopicPartition(topic, 1),
        new OffsetAndMetadata(recordsToProduce.length.toLong),
        Some(topic),
        _ => IO.unit
      )
      val records = TransactionalProducerRecords.one(
        CommittableProducerRecords(recordsToProduce, offset)
      )

      for {
        producedOrError <- TransactionalKafkaProducer
          .resource(
            TransactionalProducerSettings(
              s"id-$topic-${UUID.randomUUID()}",
              producerSettings[IO]
                .withRetries(Int.MaxValue)
            ).withTransactionTimeout(transactionTimeoutInterval - 250.millis)
          )
          .use(_.produce(records).attempt)

        consumedOrError <- IO.blocking {
          consumeFirstKeyedMessageFrom[String, String](
            topic,
            customProperties = Map(ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed")
          )
        }.attempt
      } yield expect(
        producedOrError.isLeft &&
          consumedOrError.isLeft
      )
    }
  }
}

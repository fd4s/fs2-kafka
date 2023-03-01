package fs2.kafka

import java.util
import java.util.concurrent.atomic.AtomicBoolean
import cats.effect.{IO, Resource}
import cats.effect.unsafe.implicits.global
import cats.syntax.all._
import fs2.{Chunk, Stream}
import fs2.kafka.internal.converters.collection._
import fs2.kafka.producer.MkProducer
import org.apache.kafka.clients.consumer.{ConsumerConfig, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.scalatest.EitherValues

class TransactionSpec extends BaseKafkaSpec with EitherValues {
  private def testSingle(topic: String, makeOffset: Option[Long => CommittableOffset[IO]]) = {
    createCustomTopic(topic, partitions = 3)
    val toProduce = (0 to 10).map(n => s"key-$n" -> s"value-$n")

    val produced =
      (for {
        producer <- TransactionalKafkaProducer.stream(
          TransactionalProducerSettings(
            s"id-$topic",
            producerSettings[IO]
              .withRetries(Int.MaxValue)
          )
        )
        transaction <- Stream.resource(producer.createTransaction)
        _ <- Stream.eval(IO(producer.toString should startWith("TransactionalKafkaProducer$")))
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
          .eval(records.fold(transaction.produceWithoutOffsets, transaction.produce))
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

  private def testMultiple(topic: String, makeOffset: Option[Int => CommittableOffset[IO]]) = {
    createCustomTopic(topic, partitions = 3)
    val toProduce =
      Chunk.seq((0 to 100).toList.map(n => s"key-$n" -> s"value-$n"))

    val toPassthrough = "passthrough"

    val produced =
      (for {
        producer <- TransactionalKafkaProducer.stream(
          TransactionalProducerSettings(
            s"id-$topic",
            producerSettings[IO]
              .withRetries(Int.MaxValue)
          )
        )
        transaction <- Stream.resource(producer.createTransaction)
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
            transaction.produce(records)
          case None =>
            val records = ProducerRecords(recordsToProduce, toPassthrough)
            transaction.produceWithoutOffsets(records)
        }

        result <- Stream.eval(produce)
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

  it("should be able to produce single records with offsets in a transaction") {
    withTopic { topic =>
      testSingle(
        topic,
        Some(
          i =>
            CommittableOffset[IO](
              new TopicPartition(topic, (i % 3).toInt),
              new OffsetAndMetadata(i),
              Some("group"),
              _ => IO.unit
            )
        )
      )
    }
  }

  it("should be able to produce single records without offsets in a transaction") {
    withTopic { topic =>
      testSingle(
        topic,
        None
      )
    }
  }

  it("should be able to produce multiple records with offsets in a transaction") {
    withTopic { topic =>
      testMultiple(
        topic,
        Some(
          i =>
            CommittableOffset[IO](
              new TopicPartition(topic, i % 3),
              new OffsetAndMetadata(i.toLong),
              Some("group"),
              _ => IO.unit
            )
        )
      )
    }
  }

  it("should be able to produce multiple records without offsets in a transaction") {
    withTopic { topic =>
      testMultiple(
        topic,
        None
      )
    }
  }

  it("should be able to commit offset without producing records in a transaction") {
    withTopic { topic =>
      createCustomTopic(topic, partitions = 3)

      val toPassthrough = "passthrough"
      val commitState = new AtomicBoolean(false)

      implicit val mk: MkProducer[IO] = new MkProducer[IO] {
        def apply[G[_]](settings: ProducerSettings[G, _, _]): IO[KafkaByteProducer] =
          IO.delay {
            new org.apache.kafka.clients.producer.KafkaProducer[Array[Byte], Array[Byte]](
              (settings.properties: Map[String, AnyRef]).asJava,
              new ByteArraySerializer,
              new ByteArraySerializer
            ) {
              override def sendOffsetsToTransaction(
                offsets: util.Map[TopicPartition, OffsetAndMetadata],
                consumerGroupId: String
              ): Unit = {
                commitState.set(true)
                super.sendOffsetsToTransaction(offsets, consumerGroupId)
              }
            }
          }
      }

      val transaction2 = for {
        producer <- TransactionalKafkaProducer.resource {
          TransactionalProducerSettings(
            s"id-$topic",
            producerSettings[IO]
              .withRetries(Int.MaxValue)
          )
        }
        transaction <- producer.createTransaction
        offsets = (i: Int) =>
          CommittableOffset[IO](
            new TopicPartition(topic, i % 3),
            new OffsetAndMetadata(i.toLong),
            Some("group"),
            _ => IO.unit
          )
        records = TransactionalProducerRecords(
          Chunk.seq(0 to 100).map(i => CommittableProducerRecords(Chunk.empty, offsets(i))),
          toPassthrough
        )
        results <- Resource.eval(transaction.produce(records))
      } yield results

      transaction2.use(IO.pure).map { results =>
        results.passthrough shouldBe toPassthrough
        results.records should be(empty)
        commitState.get shouldBe true
      }
    }.unsafeRunSync()
  }

  it("should not allow concurrent access to a producer during a transaction") {
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
          transaction <- Stream.resource(producer.createTransaction)
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
            }
          )
          _ <- Stream
            .eval(transaction.produce(records))
            .concurrently(
              Stream.eval(
                transaction.produce(
                  TransactionalProducerRecords.one(
                    CommittableProducerRecords.one(
                      ProducerRecord[String, String](topic, "test", "test"),
                      CommittableOffset[IO](
                        new TopicPartition(topic, 0),
                        new OffsetAndMetadata(0),
                        Some("group"),
                        _ => IO.unit
                      )
                    )
                  )
                )
              )
            )
            .concurrently(
              Stream.eval(
                producer.produce(
                  TransactionalProducerRecords.one(
                    CommittableProducerRecords.one(
                      ProducerRecord[String, String](topic, "test", "test"),
                      CommittableOffset[IO](
                        new TopicPartition(topic, 0),
                        new OffsetAndMetadata(0),
                        Some("group"),
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

  it("should fail if transaction leaks") {
    withTopic { topic =>
      createCustomTopic(topic, partitions = 3)

      val result = (for {
        globalStateRef <- IO.ref[Option[Transaction.WithoutOffsets[IO, String, String]]](None)
        makeProducer = TransactionalKafkaProducer.resource(
          TransactionalProducerSettings(
            s"id-$topic",
            producerSettings[IO]
              .withRetries(Int.MaxValue)
          )
        )
        _ <- makeProducer.flatMap(producer => producer.createTransaction).use { transaction =>
          globalStateRef.set(Some(transaction))
        }
        toProduce = ProducerRecords.one(ProducerRecord(topic, "key-0", "value-0"))
        _ <- globalStateRef.get.flatMap(_.get.produceWithoutOffsets(toProduce))
      } yield ()).attempt.unsafeRunSync()

      result.left.value shouldBe a[TransactionLeakedException]
    }
  }
}

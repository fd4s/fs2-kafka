package fs2.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.{Chunk, Stream}

final class KafkaProducerSpec extends BaseKafkaSpec {

  describe("creating producers") {
    it("should support defined syntax") {
      val settings =
        ProducerSettings[IO, String, String]

      KafkaProducer.resource[IO, String, String](settings)
      KafkaProducer[IO].resource(settings)

      KafkaProducer.stream[IO, String, String](settings)
      KafkaProducer[IO].stream(settings)

      KafkaProducer[IO].toString should startWith("ProducerPartiallyApplied$")
    }
  }

  it("should be able to produce records with single") {
    withTopic { (topic) =>
      createCustomTopic(topic, partitions = 3)
      val toProduce = (0 until 100).map(n => s"key-$n" -> s"value->$n")

      val produced =
        (for {
          producer <- KafkaProducer.stream(producerSettings[IO])
          _ <- Stream.eval(IO(producer.toString should startWith("KafkaProducer$")))
          records <- Stream.chunk(Chunk.seq(toProduce).map {
            case passthrough @ (key, value) =>
              ProducerRecords.one(ProducerRecord(topic, key, value), passthrough)
          })
          batched <- Stream
            .eval(producer.produce(records))
            .map(_.map(_.passthrough))
            .buffer(toProduce.size)
          passthrough <- Stream.eval(batched)
        } yield passthrough).compile.toVector.unsafeRunSync()

      produced should contain theSameElementsAs toProduce

      val consumed =
        consumeNumberKeyedMessagesFrom[String, String](topic, produced.size)

      consumed should contain theSameElementsAs produced
    }
  }

  it("should preserve order of records within a partition") {
    withTopic { topic =>
      createCustomTopic(topic, partitions = 1)
      val toProduce = (0 until 100).map(n => s"key-$n" -> s"value->$n")

      (for {
        producer <- KafkaProducer[IO].stream(producerSettings[IO])
        records <- Stream.chunk(Chunk.seq(toProduce).map {
          case passthrough @ (key, value) =>
            ProducerRecords.one(ProducerRecord(topic, key, value), passthrough)
        })
        _ <- Stream.eval(producer.produce(records))
      } yield ()).compile.toVector.unsafeRunSync()

      val consumed =
        consumeNumberKeyedMessagesFrom[String, String](topic, toProduce.size)

      consumed should contain theSameElementsInOrderAs toProduce
    }
  }

  it("should be able to produce records with multiple") {
    withTopic { topic =>
      createCustomTopic(topic, partitions = 3)
      val toProduce = (0 until 10).map(n => s"key-$n" -> s"value->$n").toList
      val toPassthrough = "passthrough"

      val produced =
        (for {
          producer <- KafkaProducer.stream(producerSettings[IO])
          records = ProducerRecords(toProduce.map {
            case (key, value) =>
              ProducerRecord(topic, key, value)
          }, toPassthrough)
          result <- Stream.eval(producer.produce(records).flatten)
        } yield result).compile.lastOrError.unsafeRunSync()

      val records =
        produced.records.map {
          case (record, _) =>
            record.key -> record.value
        }.toList

      assert(records == toProduce && produced.passthrough == toPassthrough)

      val consumed =
        consumeNumberKeyedMessagesFrom[String, String](topic, toProduce.size)

      consumed should contain theSameElementsAs toProduce
    }
  }

  it("should be able to produce zero records with multiple") {
    withTopic { topic =>
      createCustomTopic(topic, partitions = 3)
      val passthrough = "passthrough"

      val result =
        (for {
          producer <- KafkaProducer.stream(producerSettings[IO])
          records = ProducerRecords(Nil, passthrough)
          result <- Stream.eval(producer.produce(records).flatten)
        } yield result).compile.lastOrError.unsafeRunSync()

      assert(result.passthrough == passthrough)
    }
  }

  it("should be able to produce zero records with passthrough") {
    withTopic { topic =>
      createCustomTopic(topic, partitions = 3)
      val passthrough = "passthrough"

      val result =
        (for {
          producer <- KafkaProducer.stream(producerSettings[IO])
          result <- Stream.eval {
            producer.produce(ProducerRecords(Nil, passthrough)).flatten
          }
        } yield result).compile.lastOrError.unsafeRunSync()

      assert(result.passthrough == passthrough)
    }
  }

  it("should produce one without passthrough") {
    withTopic { topic =>
      createCustomTopic(topic, partitions = 3)
      val toProduce = ("some-key" -> "some-value")

      val produced =
        (for {
          producer <- KafkaProducer.stream(producerSettings[IO])
          _ <- Stream.eval(IO(producer.toString should startWith("KafkaProducer$")))
          batched <- Stream
            .eval(producer.produceOne_(ProducerRecord(topic, toProduce._1, toProduce._2)))
          _ <- Stream.eval(batched)
        } yield ()).compile.toVector.unsafeRunSync()

      val consumed =
        consumeNumberKeyedMessagesFrom[String, String](topic, produced.size)

      consumed should contain only toProduce
    }
  }

  it("should produce one from individual topic, key and value without passthrough") {
    withTopic { topic =>
      createCustomTopic(topic, partitions = 3)
      val toProduce = ("some-key" -> "some-value")

      val produced =
        (for {
          producer <- KafkaProducer.stream(producerSettings[IO])
          _ <- Stream.eval(IO(producer.toString should startWith("KafkaProducer$")))
          batched <- Stream
            .eval(producer.produceOne_(topic, toProduce._1, toProduce._2))
          _ <- Stream.eval(batched)
        } yield ()).compile.toVector.unsafeRunSync()

      val consumed =
        consumeNumberKeyedMessagesFrom[String, String](topic, produced.size)

      consumed should contain only toProduce
    }
  }

  it("should produce one") {
    withTopic { topic =>
      createCustomTopic(topic, partitions = 3)
      val toProduce = ("some-key" -> "some-value")
      val passthrough = "passthrough"

      val result =
        (for {
          producer <- KafkaProducer.stream(producerSettings[IO])
          _ <- Stream.eval(IO(producer.toString should startWith("KafkaProducer$")))
          batched <- Stream
            .eval(
              producer.produceOne(ProducerRecord(topic, toProduce._1, toProduce._2), passthrough)
            )
          result <- Stream.eval(batched)
        } yield result).compile.lastOrError.unsafeRunSync()

      assert(result.passthrough == passthrough)
    }
  }

  it("should produce one from individual topic, key and value") {
    withTopic { topic =>
      createCustomTopic(topic, partitions = 3)
      val toProduce = ("some-key" -> "some-value")
      val passthrough = "passthrough"

      val result =
        (for {
          producer <- KafkaProducer.stream(producerSettings[IO])
          _ <- Stream.eval(IO(producer.toString should startWith("KafkaProducer$")))
          batched <- Stream
            .eval(producer.produceOne(topic, toProduce._1, toProduce._2, passthrough))
          result <- Stream.eval(batched)
        } yield result).compile.lastOrError.unsafeRunSync()

      assert(result.passthrough == passthrough)
    }
  }

  it("should be able to produce records with multiple without passthrough") {
    withTopic { topic =>
      createCustomTopic(topic, partitions = 3)
      val toProduce = (0 until 10).map(n => s"key-$n" -> s"value->$n").toList

      val produced =
        (for {
          producer <- KafkaProducer.stream(producerSettings[IO])
          records = ProducerRecords(toProduce.map {
            case (key, value) =>
              ProducerRecord(topic, key, value)
          }, ())
          result <- Stream.eval(producer.produce_(records).flatten)
        } yield result).compile.lastOrError.unsafeRunSync()

      val records =
        produced.map {
          case (record, _) =>
            record.key -> record.value
        }.toList

      assert(records == toProduce)

      val consumed =
        consumeNumberKeyedMessagesFrom[String, String](topic, toProduce.size)

      consumed should contain theSameElementsAs toProduce
    }
  }

  it("should get metrics") {
    withTopic { topic =>
      createCustomTopic(topic, partitions = 3)

      val info =
        KafkaProducer
          .stream(producerSettings[IO])
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

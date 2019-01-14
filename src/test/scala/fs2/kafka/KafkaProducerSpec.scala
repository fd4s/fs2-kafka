package fs2.kafka

import cats.effect.IO
import cats.implicits._
import fs2.{Chunk, Stream}
import org.apache.kafka.clients.producer.ProducerRecord

final class KafkaProducerSpec extends BaseKafkaSpec {
  it("should be able to produce records with single") {
    withKafka { (config, topic) =>
      createCustomTopic(topic, partitions = 3)
      val toProduce = (0 until 100).map(n => s"key-$n" -> s"value->$n")

      val produced =
        (for {
          producer <- producerStream[IO].using(producerSettings(config))
          _ <- Stream.eval(IO(producer.toString should startWith("KafkaProducer$")))
          message <- Stream.chunk(Chunk.seq(toProduce).map {
            case passthrough @ (key, value) =>
              ProducerMessage.one(new ProducerRecord(topic, key, value), passthrough)
          })
          batched <- Stream.eval(producer.producePassthrough(message)).buffer(toProduce.size)
          passthrough <- Stream.eval(batched)
        } yield passthrough).compile.toVector.unsafeRunSync()

      produced should contain theSameElementsAs toProduce

      val consumed =
        consumeNumberKeyedMessagesFrom[String, String](topic, produced.size)

      consumed should contain theSameElementsAs produced
    }
  }

  it("should be able to produce records with multiple") {
    withKafka { (config, topic) =>
      createCustomTopic(topic, partitions = 3)
      val toProduce = (0 until 10).map(n => s"key-$n" -> s"value->$n").toList
      val toPassthrough = "passthrough"

      val produced =
        (for {
          producer <- producerStream[IO].using(producerSettings(config))
          records = toProduce.map {
            case (key, value) =>
              new ProducerRecord(topic, key, value)
          }
          message = ProducerMessage(records, toPassthrough)
          result <- Stream.eval(producer.produce(message).flatten)
        } yield result).compile.lastOrError.unsafeRunSync

      val records =
        produced.records.map {
          case (record, _) =>
            record.key -> record.value
        }

      assert(records == toProduce && produced.passthrough == toPassthrough)

      val consumed =
        consumeNumberKeyedMessagesFrom[String, String](topic, toProduce.size)

      consumed should contain theSameElementsAs toProduce
    }
  }

  it("should be able to produce zero records with multiple") {
    withKafka { (config, topic) =>
      createCustomTopic(topic, partitions = 3)
      val passthrough = "passthrough"

      val result =
        (for {
          producer <- producerStream[IO].using(producerSettings(config))
          records = List.empty[ProducerRecord[String, String]]
          message = ProducerMessage(records, passthrough)
          result <- Stream.eval(producer.produce(message).flatten)
        } yield result).compile.lastOrError.unsafeRunSync

      assert(result.passthrough == passthrough)
    }
  }

  it("should be able to produce zero records with passthrough") {
    withKafka { (config, topic) =>
      createCustomTopic(topic, partitions = 3)
      val passthrough = "passthrough"

      val result =
        (for {
          producer <- producerStream[IO].using(producerSettings(config))
          result <- Stream.eval {
            producer.produce(ProducerMessage[List].of(Nil, passthrough)).flatten
          }
        } yield result).compile.lastOrError.unsafeRunSync

      assert(result.passthrough == passthrough)
    }
  }
}

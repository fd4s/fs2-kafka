package fs2.kafka

import cats.effect.IO
import fs2.{Chunk, Stream}
import org.apache.kafka.clients.producer.ProducerRecord

final class KafkaProducerSpec extends BaseKafkaSpec {
  it("should produce all messages") {
    withKafka { (config, topic) =>
      createCustomTopic(topic, partitions = 3)
      val toProduce = (0 until 100).map(n => s"key-$n" -> s"value->$n")

      val produced =
        (for {
          producer <- producerStream[IO].using(producerSettings(config))
          message <- Stream.chunk(Chunk.seq(toProduce).map {
            case passthrough @ (key, value) =>
              ProducerMessage.single(new ProducerRecord(topic, key, value), passthrough)
          })
          batched <- Stream.eval(producer.produceBatched(message)).buffer(toProduce.size)
          passthrough <- Stream.eval(batched.map(_.passthrough))
        } yield passthrough).compile.toVector.unsafeRunSync()

      produced should contain theSameElementsAs toProduce

      val consumed =
        consumeNumberKeyedMessagesFrom[String, String](topic, produced.size)

      consumed should contain theSameElementsAs produced
    }
  }
}

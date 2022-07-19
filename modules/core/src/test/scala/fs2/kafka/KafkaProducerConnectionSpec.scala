package fs2.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream
import org.scalactic.TypeCheckedTripleEquals

final class KafkaProducerConnectionSpec extends BaseKafkaSpec with TypeCheckedTripleEquals {
  it("should allow instantiating multiple producers with different serializers") {
    withTopic { (topic) =>
      createCustomTopic(topic, partitions = 3)

      val producerRecordString = ProducerRecord(topic, "key", "value")
      val producerRecordInt = ProducerRecord(topic, 1, 2)

      val (result1, result2) =
        (for {
          settings <- Stream(producerSettings[IO])
          producerConnection <- KafkaProducerConnection.stream(settings)
          producer1 <- Stream.eval(producerConnection.withSerializersFrom(settings))
          serializer2 = Serializer.string[IO].contramap[Int](_.toString)
          producer2 = producerConnection.withSerializers(serializer2, serializer2)
          result1 <- Stream.eval(
            producer1.produce(ProducerRecords.one(producerRecordString)).flatten
          )
          result2 <- Stream.eval(producer2.produce(ProducerRecords.one(producerRecordInt)).flatten)
        } yield (result1, result2)).compile.lastOrError.unsafeRunSync()

      result1._1 should ===(producerRecordString)
      result2._1 should ===(producerRecordInt)

      val consumed =
        consumeNumberKeyedMessagesFrom[String, String](topic, 2)

      consumed should contain theSameElementsAs List("key" -> "value", "1" -> "2")
    }
  }
}

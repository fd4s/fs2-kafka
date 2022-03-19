---
id: quick-example
title: Quick Example
---

Following is an example showing how to:

- use `KafkaConsumer#records` in order to stream records from Kafka,
- use `produce` to produce newly created records to Kafka,
- use `commitBatchWithin` to commit consumed offsets in batches.

```scala mdoc
import cats.effect.{IO, IOApp}
import fs2.kafka._
import scala.concurrent.duration._

object Main extends IOApp.Simple {
  val run: IO[Unit] = {
    def processRecord(record: ConsumerRecord[String, String]): IO[(String, String)] =
      IO.pure(record.key -> record.value)

    val consumerSettings =
      ConsumerSettings[IO, String, String]
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withBootstrapServers("localhost:9092")
        .withGroupId("group")

    val producerSettings =
      ProducerSettings.default
        .withBootstrapServers("localhost:9092")

    val stream =
      KafkaConsumer.stream(consumerSettings)
        .subscribeTo("topic")
        .records
        .mapAsync(25) { committable =>
          processRecord(committable.record)
            .map { case (key, value) =>
              val record = ProducerRecord("topic", key, value)
              committable.offset -> ProducerRecords.one(record)
            }
        }.through { offsetsAndProducerRecords =>
          KafkaProducer.stream[IO, String, String](producerSettings).flatMap { producer =>
            offsetsAndProducerRecords.evalMap { 
              case (offset, producerRecord) => 
                producer.produce(producerRecord)
                  .map(_.as(offset))
            }.parEvalMap(Int.MaxValue)(identity)
          }
        }.through(commitBatchWithin(500, 15.seconds))

    stream.compile.drain
  }
}
```

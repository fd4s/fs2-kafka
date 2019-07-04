---
id: quick-example
title: Quick Example
---

Following is an example showing how to:

- use `consumerStream` in order to stream records from Kafka,
- use `produce` to produce newly created records to Kafka,
- use `commitBatchWithin` to commit consumed offsets in batches.

```scala mdoc
import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.functor._
import fs2.kafka._
import scala.concurrent.duration._

object Main extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
    def processRecord(record: ConsumerRecord[String, String]): IO[(String, String)] =
      IO.pure(record.key -> record.value)

    val consumerSettings =
      ConsumerSettings[IO, String, String]
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withBootstrapServers("localhost")
        .withGroupId("group")

    val producerSettings =
      ProducerSettings[IO, String, String]
        .withBootstrapServers("localhost")

    val stream =
      consumerStream[IO]
        .using(consumerSettings)
        .evalTap(_.subscribeTo("topic"))
        .flatMap(_.stream)
        .mapAsync(25) { committable =>
          processRecord(committable.record)
            .map { case (key, value) =>
              val record = ProducerRecord("topic", key, value)
              ProducerRecords.one(record, committable.offset)
            }
        }
        .through(produce(producerSettings))
        .map(_.passthrough)
        .through(commitBatchWithin(500, 15.seconds))

    stream.compile.drain.as(ExitCode.Success)
  }
}
```

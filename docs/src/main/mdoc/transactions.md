---
id: transactions
title: Transactions
---

Kafka transactions are supported through a [`TransactionalKafkaProducer`][transactionalkafkaproducer]. In order to use transactions, the following steps should be taken. For details on [consumers](consumers.md) and [producers](producers.md), see the respective sections.

- Create a `TransactionalProducerSettings` using a TopicPartition to generate the `transactional.id`.

- Use `withIsolationLevel(IsolationLevel.ReadCommitted)` on `ConsumerSettings`.

- Use `transactionalProducerStream` to create a producer with support for transactions.

- Create `CommittableProducerRecords` and wrap them in `TransactionalProducerRecords`.

Following is an example where transactions are used to consume, process, produce, and commit.

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
        .withIsolationLevel(IsolationLevel.ReadCommitted)
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withBootstrapServers("localhost:9092")
        .withGroupId("group")

    val producerSettings = ProducerSettings[IO, String, String]
        .withBootstrapServers("localhost:9092")

    val stream = consumerStream[IO]
        .using(consumerSettings)
        .evalTap(_.subscribeTo("topic"))
        .flatMap(_.explicitPartitionedStream)
        .flatMap {
          case (topicPartition, partitionStream) =>
            transactionalProducerStream[IO]
              .using(
                TransactionalProducerSettings(
                  "my-app",
                  topicPartition,
                  producerSettings
                )
              )
          .flatMap { producer =>
            partitionStream.mapAsync(25) { committable =>
              processRecord(committable.record)
                .map { case (key, value) =>
                  val record = ProducerRecord("topic", key, value)
                  CommittableProducerRecords.one(record, committable.offset)
                }
            }
              .groupWithin(500, 15.seconds)
              .map(TransactionalProducerRecords(_))
              .evalMap(producer.produce)
          }
        }

            

    stream.compile.drain.as(ExitCode.Success)
  }
}
```

[transactionalkafkaproducer]: @API_BASE_URL@/TransactionalKafkaProducer.html

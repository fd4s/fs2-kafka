---
id: transactions
title: Transactions
---

Kafka transactions are supported through a [`TransactionalKafkaProducer`][transactionalkafkaproducer]. In order to use transactions, the following steps should be taken. For details on [consumers](consumers.md) and [producers](producers.md), see the respective sections.

- Create a `TransactionalProducerSettings` specifying the transactional ID.

- Use `withIsolationLevel(IsolationLevel.ReadCommitted)` on `ConsumerSettings`.

- Use `TransactionalKafkaProducer.stream` to create a producer with support for transactions.

- Create `CommittableProducerRecords` and wrap them in `TransactionalProducerRecords`.

> Note that calls to `produce` are sequenced in the `TransactionalKafkaProducer` to ensure that, when used concurrently, transactions don't run into each other resulting in an invalid transaction transition exception.
>
> Because the `TransactionalKafkaProducer` waits for the record batch to be flushed and the transaction committed on the broker, this could lead to performance bottlenecks where a single producer is shared among many threads.
> To ensure the performance of `TransactionalKafkaProducer` aligns with your performance expectations when used concurrently, it is recommended you create a pool of transactional producers.

Following is an example where transactions are used to consume, process, produce, and commit.

```scala mdoc
import cats.effect.{ExitCode, IO, IOApp}
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

    val producerSettings =
      TransactionalProducerSettings(
        "transactional-id",
        ProducerSettings[IO, String, String]
          .withBootstrapServers("localhost:9092")
      )

    val stream =
      TransactionalKafkaProducer.stream(producerSettings)
        .flatMap { producer =>
          KafkaConsumer.stream(consumerSettings)
            .subscribeTo("topic")
            .stream
            .mapAsync(25) { committable =>
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

    stream.compile.drain.as(ExitCode.Success)
  }
}
```

[transactionalkafkaproducer]: @API_BASE_URL@/TransactionalKafkaProducer.html

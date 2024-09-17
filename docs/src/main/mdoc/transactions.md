---
id: transactions
title: Transactions
---

Kafka transactions are supported through a [`TransactionalKafkaProducer`][transactionalkafkaproducer]. In order to use transactions, the following steps should be taken. For details on [consumers](consumers.md) and [producers](producers.md), see the respective sections.

- Create `KafkaConsumer` then split its stream into sub-streams - one for each topic.

- Use `withIsolationLevel(IsolationLevel.ReadCommitted)` on `ConsumerSettings`.

- Create `TransactionalKafkaProducer` for each sub-stream with `TransactionalProducerSettings` to create a producer with support for transactions with **partition unique** transaction id. Kafka requires partition unique transactional ids for producer "handover" and zombie fencing.   

- Use `.withEnableIdempotence(true)` and `.withRetries(n)` where `n > 0` on `ProducerSettings`

- Create `CommittableProducerRecords` and wrap them in `TransactionalProducerRecords`.

- Combine all sub-streams into one stream.

> Note that calls to `produce` are sequenced in the `TransactionalKafkaProducer` to ensure that, when used concurrently, transactions don't run into each other resulting in an invalid transaction transition exception.
>
> Because the `TransactionalKafkaProducer` waits for the record batch to be flushed and the transaction committed on the broker, this could lead to performance bottlenecks where a single producer is shared among many threads.
> To ensure the performance of `TransactionalKafkaProducer` aligns with your performance expectations when used concurrently, it is recommended you create a pool of transactional producers.

Following is an example where transactions are used to consume, process, produce, and commit.

```scala mdoc
import scala.concurrent.duration._

import cats.effect.{IO, IOApp}
import fs2.kafka._
import fs2.Stream

import org.apache.kafka.common.TopicPartition

object Main extends IOApp.Simple {

  val run: IO[Unit] = {
    def processRecord(record: ConsumerRecord[String, String]): IO[(String, String)] =
      IO.pure(record.key -> record.value)

    val consumerSettings =
      ConsumerSettings[IO, String, String]
        .withIsolationLevel(IsolationLevel.ReadCommitted)
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withBootstrapServers("localhost:9092")
        .withGroupId("group")

    def producerSettings(partition: TopicPartition) =
      TransactionalProducerSettings(
        s"transactional-id-$partition",
        ProducerSettings[IO, String, String]
          .withBootstrapServers("localhost:9092")
          .withEnableIdempotence(true)
          .withRetries(10)
      )

    KafkaConsumer
      .stream(consumerSettings)
      .subscribeTo("topic")
      .flatMap(_.partitionsMapStream)
      .map(
        _.map { case (partition, stream) =>
          TransactionalKafkaProducer
            .stream(producerSettings(partition))
            .flatMap { producer =>
              stream
                .mapAsync(25) { committable =>
                  processRecord(committable.record).map { case (key, value) =>
                    val record = ProducerRecord("topic", key, value)
                    CommittableProducerRecords.one(record, committable.offset)
                  }
                }
                .groupWithin(500, 15.seconds)
                .evalMap(producer.produce)
            }
        }
      )
      .flatMap { partitionsMap =>
        Stream.emits(partitionsMap.toVector).parJoinUnbounded
      }
      .compile
      .drain
  }

}
```

[transactionalkafkaproducer]: @API_BASE_URL@/TransactionalKafkaProducer.html

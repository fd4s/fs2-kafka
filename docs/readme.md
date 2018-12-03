[![Travis](https://img.shields.io/travis/ovotech/fs2-kafka/master.svg)](https://travis-ci.org/ovotech/fs2-kafka) [![Codecov](https://img.shields.io/codecov/c/github/ovotech/fs2-kafka.svg)](https://codecov.io/gh/ovotech/fs2-kafka) [![Version](https://img.shields.io/badge/version-v@LATEST_VERSION@-orange.svg)](https://search.maven.org/artifact/com.ovoenergy/fs2-kafka_2.12/@LATEST_VERSION@/jar)

## FS2 Kafka
Tiny library providing an [FS2][fs2] wrapper around the official Kafka Java client.  
The API is inspired by [Alpakka Kafka][alpakka-kafka], and migration should be relatively easy.

This is a new project under active development. Feedback and contributions are welcome.

### Getting Started
To get started with [sbt][sbt], simply add the following line to your `build.sbt` file.

```scala
libraryDependencies += "com.ovoenergy" %% "fs2-kafka" % "@LATEST_VERSION@"
```

The library is published for Scala 2.11 and 2.12.

Backwards binary compatibility for the library is guaranteed between patch versions.  
For example, `@LATEST_MINOR_VERSION@x` is backwards binary compatible with `@LATEST_MINOR_VERSION@y` for any `x > y`.

### Usage
Start with `import fs2.kafka._` and use `consumerStream` and `producerStream` to create a consumer and producer, by providing a `ConsumerSettings` and `ProducerSettings`, respectively. The consumer is similar to `committableSource` in Alpakka Kafka, wrapping records in `CommittableMessage`. The producer accepts records wrapped in `ProducerMessage`, allowing offsets, and other elements, as passthrough values.

```scala mdoc
import cats.data.NonEmptyList
import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.functor._
import fs2.kafka._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object Main extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
    val consumerSettings = (executionContext: ExecutionContext) =>
      ConsumerSettings(
        keyDeserializer = new StringDeserializer,
        valueDeserializer = new StringDeserializer,
        executionContext = executionContext
      )
      .withAutoOffsetReset(AutoOffsetReset.Earliest)
      .withBootstrapServers("localhost")
      .withGroupId("group")

    val producerSettings =
      ProducerSettings(
        keySerializer = new StringSerializer,
        valueSerializer = new StringSerializer,
      )
      .withBootstrapServers("localhost")

    val topics =
      NonEmptyList.one("topic")

    def processRecord(record: ConsumerRecord[String, String]): IO[(String, String)] =
      IO.pure(record.key -> record.value)

    val stream =
      for {
        executionContext <- consumerExecutionContextStream[IO]
        consumer <- consumerStream[IO].using(consumerSettings(executionContext))
        producer <- producerStream[IO].using(producerSettings)
        _ <- consumer.subscribe(topics)
        _ <- consumer.stream
          .mapAsync(25)(message =>
            processRecord(message.record)
              .map {
                case (key, value) =>
                  val record = new ProducerRecord("topic", key, value)
                  ProducerMessage.single(record, message.committableOffset)
              })
            .evalMap(producer.produceBatched)
            .map(_.map(_.passthrough))
            .to(commitBatchWithinF(500, 15.seconds))
      } yield ()

    stream.compile.drain.as(ExitCode.Success)
  }
}
```

[alpakka-kafka]: https://github.com/akka/alpakka-kafka
[fs2]: http://fs2.io/
[sbt]: https://www.scala-sbt.org

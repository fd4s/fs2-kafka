## FS2 Kafka
Tiny library providing an [FS2][fs2] wrapper around the official Kafka Java client.  
The API is inspired by [Alpakka Kafka][alpakka-kafka], and migration should be relatively easy.

### Getting Started
To get started with [sbt][sbt], simply add the following lines to your `build.sbt` file.

```scala
resolvers += Resolver.bintrayRepo("ovotech", "maven")

libraryDependencies += "com.ovoenergy" %% "fs2-kafka" % "0.2"
```

The library is published for Scala 2.11 and 2.12.

### Usage
Start with `import fs2.kafka._` and use `consumerStream` and `producerStream` to create a consumer and producer, by providing a `ConsumerSettings` and `ProducerSettings`, respectively. The consumer is similar to `committableSource` in Alpakka Kafka, wrapping records in `CommittableMessage`. The producer accepts records wrapped in `ProducerMessage`, allowing offsets, and other elements, as passthrough values.

```scala
import cats.data.NonEmptyList
import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.functor._
import fs2.kafka._
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

object Main extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
    val consumerSettings =
      ConsumerSettings(
        keyDeserializer = new StringDeserializer,
        valueDeserializer = new StringDeserializer,
        nativeSettings = Map(
          ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost",
          ConsumerConfig.GROUP_ID_CONFIG -> "group"
        )
      )

    val producerSettings =
      ProducerSettings(
        keySerializer = new StringSerializer,
        valueSerializer = new StringSerializer,
        nativeSettings = Map(
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost"
        )
      )

    val topics =
      NonEmptyList.one("topic")

    def processRecord(record: ConsumerRecord[String, String]): IO[(String, String)] =
      IO.pure(record.key -> record.value)

    val stream =
      for {
        consumer <- consumerStream[IO].using(consumerSettings)
        producer <- producerStream[IO].using(producerSettings)
        _ <- consumer.subscribe(topics)
        _ <- consumer.stream {
          _.mapAsync(25)(message =>
            processRecord(message.record)
              .map {
                case (key, value) =>
                  val record = new ProducerRecord("topic", key, value)
                  ProducerMessage.single(record, message.committableOffset)
            })
            .evalMap(producer.produceWithBatching)
            .bufferAll // Let all records in batch be sent to the producer before committing
            .evalMap(_.map(_.passthrough))
            .fold(CommittableOffsetBatch.empty[IO])(_ updated _)
            .evalMap(_.commit)
        }
      } yield ()

    stream.compile.drain.as(ExitCode.Success)
  }
}
```

[alpakka-kafka]: https://github.com/akka/alpakka-kafka
[fs2]: http://fs2.io/
[sbt]: https://www.scala-sbt.org

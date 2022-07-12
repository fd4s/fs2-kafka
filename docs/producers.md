# Producers

Producers support publishing of records. Producers make use of the [Java Kafka producer][@:api(org.apache.kafka.clients.producer.KafkaProducer)], which becomes especially important for [settings](#settings).

The following imports are assumed throughout this page.

```scala mdoc:silent
import cats.effect._
import cats.syntax.all._
import fs2.kafka._
import scala.concurrent.duration._
```

## Serializers

[`Serializer`][@:api(fs2.kafka.Serializer)] describes functional composable serializers for record keys and values. We generally require two serializers: one for the record key and one for the record value. Serializers are provided implicitly for many standard library types, including:

- `Array[Byte]`, `Double`, `Float`, `Int`, `Long`, `Short`, `String`, and `UUID`.

There are also serializers for types which carry special meaning:

- `Option[A]` to serialize occurrances of `None` as `null`, and

- `Unit` to ignore the value and always serialize as `null`.

For more involved types, we need to resort to custom serializers.

### Custom Serializers

`Serializer[F[_], A]` describes a function `A => F[Array[Byte]]`, while also having access to the topic name and record @:api(fs2.kafka.Headers). There are many [functions][@:api(fs2.kafka.Serializer$)] available for creating custom serializers, with the most basic one being `instance`, which simply creates a serializer from a provided function.

```scala mdoc:silent
Serializer.instance[IO, String] {
  (_, _, s) =>
    IO.pure(s.getBytes("UTF-8"))
}
```

If the serializer only needs access to the bytes, like in the case above, use `lift`.

```scala mdoc:silent
Serializer.lift[IO, String](s => IO.pure(s.getBytes("UTF-8")))
```

To support different serializers for different topics, use `topic` to pattern match on the topic name.

```scala mdoc:silent
Serializer.topic[IO, Int] {
  case "first"  => Serializer[IO, String].contramap(_.show)
  case "second" => Serializer[IO, Int]
}
```

For unmatched topics, an [`UnexpectedTopicException`][@:api(fs2.kafka.UnexpectedTopicException)] is raised.

Use `headers` for different deserializers depending on record headers.

```scala mdoc:silent
Serializer.headers[IO, Int] { headers =>
  headers("format").map(_.as[String]) match {
    case Some("string") => Serializer[IO, String].contramap(_.show)
    case Some("int")    => Serializer[IO, Int]
    case Some(format)   => Serializer.failWith(s"unknown format: $format")
    case None           => Serializer.failWith("format header is missing")
  }
}
```

In the example above, `failWith` raises a @:api(fs2.kafka.SerializationException) with the provided message.

### Java Interoperability

If we have a Java Kafka serializer, use `delegate` to create a `Serializer`.

```scala mdoc:silent
Serializer.delegate[IO, String] {
  new KafkaSerializer[String] {
    def serialize(topic: String, value: String): Array[Byte] =
      value.getBytes("UTF-8")
  }
}
```

If the serializer performs _side effects_, follow with `suspend` to capture them properly.

```scala mdoc:silent
Serializer.delegate[IO, String] {
   new KafkaSerializer[String] {
    def serialize(topic: String, value: String): Array[Byte] = {
      println(s"serializing record on topic $topic")
      value.getBytes("UTF-8")
    }
  }
}.suspend
```

Note that `close` and `configure` won't be called for the delegates.

## Settings

In order to create a @:api(fs2.kafka.KafkaProducer), we first need to create @:api(fs2.kafka.ProducerSettings). At the very minimum, settings include the effect type to use, and the key and value serializers. More generally, [`ProducerSettings`][@:api(fs2.kafka.ProducerSettings)] contain everything necessary to create a [`KafkaProducer`][@:api(fs2.kafka.KafkaProducer)]. If serializers are available implicitly for the key and value type, we can use the syntax in the following example.

```scala mdoc:silent
val producerSettings =
  ProducerSettings[IO, String, String]
    .withBootstrapServers("localhost:9092")
```

We can also specify the serializers explicitly when necessary.

```scala mdoc:silent
ProducerSettings(
  keySerializer = Serializer[IO, String],
  valueSerializer = Serializer[IO, String]
).withBootstrapServers("localhost:9092")
```

[`ProducerSettings`][@:api(fs2.kafka.ProducerSettings)] provides functions for configuring both the Java Kafka producer and options specific to the library. If functions for configuring certain properties of the Java Kafka producer is missing, we can instead use `withProperty` or `withProperties` together with constants from [`ProducerConfig`][@:api(org.apache.kafka.clients.producer.ProducerConfig)]. Available properties for the Java Kafka producer are described in the [documentation](http://kafka.apache.org/documentation/#producerconfigs).

### Default Settings

The following Java Kafka producer properties are overridden by default.

- `max.retries` is set to `0`, to avoid the risk of records being produced out-of-order. If we don't need to produce records in-order, then this can be set to some positive integer value. An alternative is to enable retries and use `withMaxInFlightRequestsPerConnection(1)` or `withEnableIdempotence(true)`. The blog post [Does Kafka really guarantee the order of messages?](https://blog.softwaremill.com/does-kafka-really-guarantee-the-order-of-messages-3ca849fd19d2) provides more detail on this topic.

The following settings are specific to the library.

- `withCloseTimeout` controls the timeout when waiting for producer shutdown. Default is 60 seconds.

- `withParallelism` sets the max number of `ProducerRecords` to produce in the same batch when using the `produce` pipe. Default is 10000.

- `withCreateProducer` changes how the underlying Java Kafka producer is created. The default merely creates a Java `KafkaProducer` instance using set properties, but this function allows overriding the behaviour for e.g. testing purposes.

## Producer Creation

Once [`ProducerSettings`][@:api(fs2.kafka.ProducerSettings)] is defined, use `KafkaProducer.stream` to create a [`KafkaProducer`][@:api(fs2.kafka.KafkaProducer)] instance.

```scala mdoc:silent
object ProducerExample extends IOApp.Simple {
  val run: IO[Unit] =
    KafkaProducer.stream(producerSettings).compile.drain
}
```

There is also `KafkaProducer.resource` for when it's preferable to work with `Resource`. Both these functions create an underlying Java Kafka producer. They both also guarantee resource cleanup, i.e. closing the Kafka producer instance.

In the example above, we simply create the producer and then immediately shutdown after resource cleanup. [`KafkaProducer`][@:api(fs2.kafka.KafkaProducer)] only supports producing records, and there is a separate producer available to support [transactions](transactions.md).

## Producing Records

If we're only producing records in one part of our stream, using `produce` is convenient.

```scala mdoc:silent
val consumerSettings =
  ConsumerSettings[IO, String, String]
    .withAutoOffsetReset(AutoOffsetReset.Earliest)
    .withBootstrapServers("localhost:9092")
    .withGroupId("group")

object ProduceExample extends IOApp.Simple {
  val run: IO[Unit] =
    KafkaConsumer.stream(consumerSettings)
      .subscribeTo("topic")
      .records
      .map { committable =>
        val key = committable.record.key
        val value = committable.record.value
        val record = ProducerRecord("topic", key, value)
        ProducerRecords.one(record, committable.offset)
      }
      .through(KafkaProducer.pipe(producerSettings))
      .compile.drain
}
```

In the stream above, we're simply producing the records we receive back to the topic.

The `produce` function creates a `KafkaProducer` and produces records in `ProducerRecords`. Note that `ProducerRecords` support multiple records and a passthrough value, `committable.offset`. Once all records have been produced in the `ProducerRecords`, the passthrough will be emitted.

If we're producing in multiple places in our stream, we can create the `KafkaProducer` ourselves, and pass it to the `produce` function. Every `produce` allow up to `ProducerSettings#parallelism` instances of `ProducerRecords` to be batched together in the same batch.

```scala mdoc:silent
object PartitionedProduceExample extends IOApp.Simple {
  val run: IO[Unit] =
    KafkaProducer.stream(producerSettings)
      .flatMap { producer =>
        KafkaConsumer.stream(consumerSettings)
          .subscribeTo("topic")
          .partitionedRecords
          .map { partition =>
            partition
              .map { committable =>
                val key = committable.record.key
                val value = committable.record.value
                val record = ProducerRecord("topic", key, value)
                ProducerRecords.one(record, committable.offset)
              }
              .through(KafkaProducer.pipe(producerSettings, producer))
          }
          .parJoinUnbounded
      }
      .compile.drain
}
```

If we need more control of how records are produced, we can use `KafkaProducer#produce`. The function returns two effects, e.g. `IO[IO[...]]`, where the first effect puts the records in the producer's buffer, and the second effects waits for the records to have been sent.

```scala mdoc:silent
object KafkaProducerProduceExample extends IOApp.Simple {
  val run: IO[Unit] =
    KafkaProducer.stream(producerSettings)
      .flatMap { producer =>
        KafkaConsumer.stream(consumerSettings)
          .subscribeTo("topic")
          .records
          .map { committable =>
            val key = committable.record.key
            val value = committable.record.value
            val record = ProducerRecord("topic", key, value)
            ProducerRecords.one(record, committable.offset)
          }
          .evalMap(producer.produce)
          .groupWithin(500, 15.seconds)
          .evalMap(_.sequence)
      }
      .compile.drain
}
```

The example above puts 500 records in the producer's buffer or however many can be put in the buffer every 15 seconds, and then waits for those records to finish sending before continuing. Using `produce` allows more precise control of how records are put in the buffer and when we wait for records to send.

Sometimes there is a need to wait for individual `ProducerRecords` to send. In this case, we can `flatten` the result of `produce` to both send the record and wait for the send to complete. Note that this should generally be avoided, as it achieves poor performance.

```scala mdoc:silent
object KafkaProducerProduceFlattenExample extends IOApp.Simple {
  val run: IO[Unit] = {
    KafkaProducer.stream(producerSettings)
      .flatMap { producer =>
        KafkaConsumer.stream(consumerSettings)
          .subscribeTo("topic")
          .records
          .map { committable =>
            val key = committable.record.key
            val value = committable.record.value
            val record = ProducerRecord("topic", key, value)
            ProducerRecords.one(record, committable.offset)
          }
          .evalMap { record =>
            producer.produce(record).flatten
          }
      }
      .compile.drain
  }
}
```

[headers]: @:api(fs2.kafka.Headers)
[@:api(org.apache.kafka.clients.producer.KafkaProducer)]: @:api(org.apache.kafka.clients.producer.KafkaProducer)
[kafkaproducer]: @:api(fs2.kafka.KafkaProducer)
[producerconfig]: @:api(org.apache.kafka.clients.producer.ProducerConfig)
[producersettings]: @:api(fs2.kafka.ProducerSettings)
[serializationexception]: @:api(fs2.kafka.SerializationException)
[serializer]: @:api(fs2.kafka.Serializer)
[serializer$]: @:api(fs2.kafka.Serializer$)
[unexpectedtopicexception]: @:api(fs2.kafka.UnexpectedTopicException)

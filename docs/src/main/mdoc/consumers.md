---
id: consumers
title: Consumers
---

Consumers support subscribing to topics, record streaming and deserialization, as well as miscellaneous utility functionality, such as seeking to offsets, or checking what the end offsets are for a topic. Consumers make use of the [Java Kafka consumer][java-kafka-consumer], which becomes especially important for [settings](#settings). For consumer implementation details, refer to the [technical details](technical-details.md) section.

The following imports are assumed throughout this page.

```scala mdoc:silent
import cats.effect._
import cats.implicits._
import fs2.kafka._
import scala.concurrent.duration._
```

## Deserializers

[`Deserializer`][deserializer] describes functional composable deserializers for record keys and values. We generally require two deserializers: one for the record key and one for the record value. Deserializers are provided implicitly for many standard library types, including:

- `Array[Byte]`, `Double`, `Float`, `Int`, `Long`, `Short`, `String`, and `UUID`.

There are also deserializers for types which carry special meaning:

- `Option[A]` to deserialize occurrances of `null` as `None`, and

- `Unit` to ignore the serialized bytes and always use `()`.

For more involved types, we need to resort to custom deserializers.

### Custom Deserializers

`Deserializer[F[_], A]` describes a function `Array[Byte] => F[A]`, while also having access to the topic name and record [`Headers`][headers]. There are many [functions][deserializer$] available for creating custom deserializers, with the most basic one being `instance`, which simply creates a deserializer from a provided function.

```scala mdoc:silent
Deserializer.instance {
  (topic, headers, bytes) =>
    IO.pure(bytes.dropWhile(_ == 0))
}
```

If the deserializer only needs access to the bytes, like in the case above, use `lift`.

```scala mdoc:silent
Deserializer.lift(bytes => IO.pure(bytes.dropWhile(_ == 0)))
```

To support different deserializers for different topics, use `topic` to pattern match on the topic name.

```scala mdoc:silent
Deserializer.topic[IO, String] {
  case "first"  => Deserializer[IO, String]
  case "second" => Deserializer[IO, Int].map(_.show)
}
```

For unmatched topics, an [`UnexpectedTopicException`][unexpectedtopicexception] is raised.

Use `headers` for different deserializers depending on record headers.

```scala mdoc:silent
Deserializer.headers[IO, String] { headers =>
  headers("format").map(_.as[String]) match {
    case Some("string") => Deserializer[IO, String]
    case Some("int")    => Deserializer[IO, Int].map(_.show)
    case Some(format)   => Deserializer.failWith(s"unknown format: $format")
    case None           => Deserializer.failWith("format header is missing")
  }
}
```

In the example above, `failWith` raises a [`DeserializationException`][deserializationexception] with the provided message.

### Java Interoperability

If we have a Java Kafka deserializer, use `delegate` to create a `Deserializer`.

```scala mdoc:silent
Deserializer.delegate[IO, String] {
  new KafkaDeserializer[String] {
    def deserialize(topic: String, data: Array[Byte]): String =
      new String(data)
  }
}
```

If the deserializer performs _side effects_, follow with `suspend` to capture them properly.

```scala mdoc:silent
Deserializer.delegate[IO, String] {
   new KafkaDeserializer[String] {
    def deserialize(topic: String, data: Array[Byte]): String = {
      println(s"deserializing record on topic $topic")
      new String(data)
    }
  }
}.suspend
```

Note that `close` and `configure` won't be called for the delegates.

## Settings

In order to create a [`KafkaConsumer`][kafkaconsumer], we first need to create [`ConsumerSettings`][consumersettings]. At the very minimum, settings include the effect type to use, and the key and value deserializers. More generally, [`ConsumerSettings`][consumersettings] contain everything necessary to create a [`KafkaConsumer`][kafkaconsumer]. If deserializers are available implicitly for the key and value type, we can use the syntax in the following example.

```scala mdoc:silent
val consumerSettings =
  ConsumerSettings[IO, String, String]
    .withAutoOffsetReset(AutoOffsetReset.Earliest)
    .withBootstrapServers("localhost:9092")
    .withGroupId("group")
```

We can also specify the deserializers explicitly when necessary.

```scala mdoc:silent
ConsumerSettings(
  keyDeserializer = Deserializer[IO, String],
  valueDeserializer = Deserializer[IO, String]
).withAutoOffsetReset(AutoOffsetReset.Earliest)
 .withBootstrapServers("localhost:9092")
 .withGroupId("group")
```

[`ConsumerSettings`][consumersettings] provides functions for configuring both the Java Kafka consumer and options specific to the library. If functions for configuring certain properties of the Java Kafka consumer is missing, we can instead use `withProperty` or `withProperties` together with constants from [`ConsumerConfig`][consumerconfig]. Available properties for the Java Kafka consumer are described in the [documentation](http://kafka.apache.org/documentation/#consumerconfigs).

### Default Settings

The following Java Kafka consumer properties are overridden by default.

- `auto.offset.reset` is set to `none`, to avoid the surprising `latest` default.

- `enable.auto.commit` is set to `false`, since offset commits are managed manually.

Use `withAutoOffsetReset` and `withEnableAutoCommit` to change these properties.

In addition, there are several settings specific to the library.

- `withCloseTimeout` controls the timeout when waiting for consumer shutdown. Default is 20 seconds.

- `withCommitRecovery` defines how offset commit exceptions are recovered. See [`CommitRecovery.Default`][commitrecovery-default].

- `withCommitTimeout` sets the timeout for offset commits. Default is 15 seconds.

- `withCreateConsumer` changes how the underlying Java Kafka consumer is created. The default merely creates a Java `KafkaConsumer` instance using set properties, but this function allows overriding the behaviour for e.g. testing purposes.

- `withBlocker` sets the `Blocker` on which blocking Java Kafka consumer functions are executed. Unless specified, a default fixed single-thread pool is created as part of consumer initialization, with the thread name using the `fs2-kafka-consumer` prefix.

- `withMaxPrefetchBatches` adjusts the maximum number of record batches per topic-partition to prefetch before backpressure is applied. The default is 2, meaning there can be up to 2 record batches per topic-partition waiting to be processed.

- `withPollInterval` alters how often consumer `poll` should take place. Default is 50 milliseconds.

- `withPollTimeout` modifies for how long `poll` is allowed to block. Default is 50 milliseconds.

- `withRecordMetadata` defines what metadata to include in `OffsetAndMetadata` for consumed records. This effectively allows us to store metadata along with offsets when committed to Kafka. The default is for no metadata to be included.

## Consumer Creation

Once [`ConsumerSettings`][consumersettings] is defined, use `KafkaConsumer.stream` to create a [`KafkaConsumer`][kafkaconsumer] instance.

```scala mdoc:silent
object ConsumerExample extends IOApp {
  def run(args: List[String]): IO[ExitCode] = {
    val stream =
      KafkaConsumer.stream(consumerSettings)

    stream.compile.drain.as(ExitCode.Success)
  }
}
```

There is also `KafkaConsumer.resource` for when it's preferable to work with `Resource`. Both these functions create an underlying Java Kafka consumer and start work in the background to support record streaming. In addition, they both also guarantee resource cleanup (closing the Kafka consumer and stopping background work).

In the example above, we simply create the consumer and then immediately shutdown after resource cleanup. [`KafkaConsumer`][kafkaconsumer] supports much of the Java Kafka consumer functionality in addition to record streaming, but for streaming records, we first have to subscribe to a topic.

## Topic Subscription

We can use `subscribe` with a non-empty collection of topics, or `subscribeTo` for varargs support. There is also an option to `subscribe` using a `Regex` regular expression for the topic names, in case the exact topic names are not known up-front.

```scala mdoc:silent
object ConsumerSubscribeExample extends IOApp {
  def run(args: List[String]): IO[ExitCode] = {
    val stream =
      KafkaConsumer.stream(consumerSettings)
        .evalTap(_.subscribeTo("topic"))

    stream.compile.drain.as(ExitCode.Success)
  }
}
```

Note that only automatic partition assignment is supported. Like in the [consumer creation](#consumer-creation) section, the example above only creates a consumer (guaranteeing resource cleanup) and subscribes to a topic. No records are yet streamed from the topic, for which we'll have to use `stream` or `partitionedStream`.

## Record Streaming

Once subscribed to at least one topic, we can use `stream` for a `Stream` of [`CommittableConsumerRecord`][committableconsumerrecord]s. Each record contains a deserialized [`ConsumerRecord`][consumerrecord], as well as a [`CommittableOffset`][committableoffset] for managing [offset commits](#offset-commits). Streams guarantee records in topic-partition order, but not ordering across partitions. This is the same ordering guarantee that Kafka provides.

```scala mdoc:silent
object ConsumerStreamExample extends IOApp {
  def run(args: List[String]): IO[ExitCode] = {
    val stream =
      KafkaConsumer.stream(consumerSettings)
        .evalTap(_.subscribeTo("topic"))
        .flatMap(_.stream)

    stream.compile.drain.as(ExitCode.Success)
  }
}
```

Note that this is an infinite stream, meaning it will only terminate if it's interrupted, errors, or if we turn it into a finite stream (using e.g. `take`). It's usually desired that consumer streams keep running indefinitely, so that incoming records get processed quickly &mdash; one notable exception being when testing streams.

When using `stream`, records on all assigned partitions end up in the same `Stream`. Depending on how records are processed, we might want to separate records per topic-partition. This exact functionality is provided by `partitionedStream`.

```scala mdoc:silent
object ConsumerPartitionedStreamExample extends IOApp {
  def run(args: List[String]): IO[ExitCode] = {
    def processRecord(record: ConsumerRecord[String, String]): IO[Unit] =
      IO(println(s"Processing record: $record"))

    val stream =
      KafkaConsumer.stream(consumerSettings)
        .evalTap(_.subscribeTo("topic"))
        .flatMap(_.partitionedStream)
        .map { partitionStream =>
          partitionStream
            .evalMap { committable =>
              processRecord(committable.record)
            }
        }
        .parJoinUnbounded

    stream.compile.drain.as(ExitCode.Success)
  }
}
```

The `partitionStream` in the example above is a `Stream` of records for a single topic-partition. We define the processing per topic-partition rather than across all partitions, as was the case with `stream`. The example will run `processRecord` on every record, one-at-a-time in-order per topic-partition. However, multiple partitions are processed at the same time when using `parJoinUnbounded`.

Note that we have to use `parJoinUnbounded` here so that all partitions are processed. While `parJoinUnbounded` doesn't limit the number of streams running concurrently, the actual limit is the number of assigned partitions. In fact, `stream` is just an alias for `partitionedStream.parJoinUnbounded`.

When processing of records is independent of each other, as is the case with `processRecord` above, it's often easier and more performant to use `stream` and `mapAsync`, as seen in the example below. Generally, it's crucial to ensure there are no data races between processing of any two records.

```scala mdoc:silent
object ConsumerMapAsyncExample extends IOApp {
  def run(args: List[String]): IO[ExitCode] = {
    def processRecord(record: ConsumerRecord[String, String]): IO[Unit] =
      IO(println(s"Processing record: $record"))

    val stream =
      KafkaConsumer.stream(consumerSettings)
        .evalTap(_.subscribeTo("topic"))
        .flatMap(_.stream)
        .mapAsync(25) { committable =>
          processRecord(committable.record)
        }

    stream.compile.drain.as(ExitCode.Success)
  }
}
```

## Offset Commits

Offsets commits are managed manually, which is important for ensuring at-least-once delivery. This means that, by [default](#default-settings), automatic offset commits are disabled. If you're sure you don't need at-least-once delivery, you can re-enable automatic offset commits using `withEnableAutoCommit` on [`ConsumerSettings`][consumersettings], and then ignore the [`CommittableOffset`][committableoffset] part of [`CommittableConsumerRecord`][committableconsumerrecord], keeping only the [`ConsumerRecord`][consumerrecord].

Offset commits are usually done in batches for performance reasons. We normally don't need to commit every offset, but only the last processed offset. There is a trade-off in how much reprocessing we have to do when we restart versus the performance implication of committing more frequently. Depending on our situation, we'll then choose an appropriate frequency for offset commits.

We should keep the [`CommittableOffset`][committableoffset] in our `Stream` once we've finished processing the record. For at-least-once delivery, it's essential that offset commits preserve topic-partition ordering, so we have to make sure we keep offsets in the same order as we receive them. There are then several functions available for common batch committing scenarios, like `commitBatch`, `commitBatchOption`, and `commitBatchWithin`.

```scala mdoc:silent
object ConsumerCommitBatchExample extends IOApp {
  def run(args: List[String]): IO[ExitCode] = {
    def processRecord(record: ConsumerRecord[String, String]): IO[Unit] =
      IO(println(s"Processing record: $record"))

    val stream =
      KafkaConsumer.stream(consumerSettings)
        .evalTap(_.subscribeTo("topic"))
        .flatMap(_.stream)
        .mapAsync(25) { committable =>
          processRecord(committable.record)
            .as(committable.offset)
        }
        .through(commitBatchWithin(500, 15.seconds))

    stream.compile.drain.as(ExitCode.Success)
  }
}
```

The example above commits once every 500 offsets or 15 seconds, whichever happens first. Alternatively, `commitBatch` uses the underlying chunking of the `Stream`, committing once every `Chunk`, while the `commitBatchOption` function does the same except when offsets are wrapped in `Option`.

The batch commit functions uses [`CommittableOffsetBatch`][committableoffsetbatch] and provided [functions][committableoffsetbatch$] for batching offsets. For more involved batch commit scenarios, we can use [`CommittableOffsetBatch`][committableoffsetbatch] to batch offsets, while having custom logic to determine batch frequency.

For at-least-once delivery, offset commit has to be the last step in the stream. Anything that happens after offset commit cannot be part of the at-least-once guarantee. This is the main reason why batch commit functions return `Unit`, as they should always be the last part of the stream definition.

If we're sure we need to commit every offset, we can `commit` individual [`CommittableOffset`][committableoffset]s. Note there is a substantial performance implication to committing every offset, and it should be avoided when possible. The approach also limits parallelism, since offset commits need to preserve topic-partition ordering.

[commitrecovery-default]: @API_BASE_URL@/CommitRecovery$.html#Default:fs2.kafka.CommitRecovery
[committableconsumerrecord]: @API_BASE_URL@/CommittableConsumerRecord.html
[committableoffset]: @API_BASE_URL@/CommittableOffset.html
[committableoffsetbatch]: @API_BASE_URL@/CommittableOffsetBatch.html
[committableoffsetbatch$]: @API_BASE_URL@/CommittableOffsetBatch$.html
[consumerconfig]: @KAFKA_API_BASE_URL@/?org/apache/kafka/clients/consumer/ConsumerConfig.html
[consumerrecord]: @API_BASE_URL@/ConsumerRecord.html
[consumersettings]: @API_BASE_URL@/ConsumerSettings.html
[deserializer]: @API_BASE_URL@/Deserializer.html
[deserializer$]: @API_BASE_URL@/Deserializer$.html
[headers]: @API_BASE_URL@/Headers.html
[java-kafka-consumer]: @KAFKA_API_BASE_URL@/?org/apache/kafka/clients/consumer/KafkaConsumer.html
[kafkaconsumer]: @API_BASE_URL@/KafkaConsumer.html
[deserializationexception]: @API_BASE_URL@/DeserializationException.html
[unexpectedtopicexception]: @API_BASE_URL@/UnexpectedTopicException.html

---
id: modules
title: Modules
---

The following sections describe the additional modules.

## Vulcan

The `@VULCAN_MODULE_NAME@` module provides [Avro](https://avro.apache.org) serialization support using [Vulcan](https://ovotech.github.io/vulcan).

We start by defining the type we want to serialize or deserialize, and create a `Codec`.

```scala mdoc
import cats.implicits._
import vulcan.Codec

final case class Person(name: String, age: Option[Int])

implicit val personCodec: Codec[Person] =
  Codec.record(
    name = "Person",
    namespace = Some("com.example")
  ) { field =>
    (
      field("name", _.name),
      field("age", _.age)
    ).mapN(Person(_, _))
  }
```

We then define `AvroSettings`, describing the schema registry settings.

```scala mdoc:silent
import cats.effect.IO
import fs2.kafka.vulcan.{Auth, AvroSettings, SchemaRegistryClientSettings}

val avroSettings =
  AvroSettings {
    SchemaRegistryClientSettings[IO]("http://localhost:8081")
      .withAuth(Auth.Basic("username", "password"))
  }
```

We can then create a `Serializer` and `Deserializer` instance for `Person`.

```scala mdoc:silent
import fs2.kafka.{Deserializer, RecordDeserializer, RecordSerializer, Serializer}
import fs2.kafka.vulcan.{avroDeserializer, avroSerializer}

implicit val personSerializer: RecordSerializer[IO, Person] =
  avroSerializer[Person].using(avroSettings)

implicit val personDeserializer: RecordDeserializer[IO, Person] =
  avroDeserializer[Person].using(avroSettings)
```

Finally, we can create settings, passing the `Serializer`s and `Deserializer`s implicitly.

```scala mdoc:silent
import fs2.kafka.{AutoOffsetReset, ConsumerSettings, ProducerSettings}

val consumerSettings =
  ConsumerSettings[IO, String, Person]
    .withAutoOffsetReset(AutoOffsetReset.Earliest)
    .withBootstrapServers("localhost")
    .withGroupId("group")

val producerSettings =
  ProducerSettings[IO, String, Person]
    .withBootstrapServers("localhost")
```

If we prefer, we can instead specificy the `Serializer`s and `Deserializer`s explicitly.

```scala mdoc:silent
import fs2.kafka.{Deserializer, Serializer}

ConsumerSettings(
  keyDeserializer = Deserializer[IO, String],
  valueDeserializer = personDeserializer
).withAutoOffsetReset(AutoOffsetReset.Earliest)
 .withBootstrapServers("localhost")
 .withGroupId("group")

ProducerSettings(
  keySerializer = Serializer[IO, String],
  valueSerializer = personSerializer
).withBootstrapServers("localhost")
```

### Sharing Client

When creating `AvroSettings` with `SchemaRegistryClientSettings`, one schema registry client will be created per `Serializer` or `Deserializer`. For many cases, this is completely fine, but it's possible to reuse a single client for multiple `Serializer`s and `Deserializer`s.

To share a `SchemaRegistryClient`, we first create it and then pass it to `AvroSettings`.

```scala mdoc:silent
val avroSettingsSharedClient: IO[AvroSettings[IO]] =
  SchemaRegistryClientSettings[IO]("http://localhost:8081")
    .withAuth(Auth.Basic("username", "password"))
    .createSchemaRegistryClient
    .map(AvroSettings(_))
```

We can then create multiple `Serializer`s and `Deserializer`s using the `AvroSettings`.

```scala mdoc:silent
avroSettingsSharedClient.map { avroSettings =>
  val personSerializer: RecordSerializer[IO, Person] =
    avroSerializer[Person].using(avroSettings)

  val personDeserializer: RecordDeserializer[IO, Person] =
    avroDeserializer[Person].using(avroSettings)

  val consumerSettings =
    ConsumerSettings(
      keyDeserializer = Deserializer[IO, String],
      valueDeserializer = personDeserializer
    ).withAutoOffsetReset(AutoOffsetReset.Earliest)
    .withBootstrapServers("localhost")
    .withGroupId("group")

 val producerSettings =
  ProducerSettings(
    keySerializer = Serializer[IO, String],
    valueSerializer = personSerializer
  ).withBootstrapServers("localhost")

  (consumerSettings, producerSettings)
}
```

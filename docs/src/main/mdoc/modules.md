---
id: modules
title: Modules
---

The following sections describe the additional modules.

## Vulcan

The `@VULCAN_MODULE_NAME@` module provides Avro serialization support.

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

We then define `AvroSettings` for `Person`, describing the schema registry settings.

```scala mdoc:silent
import cats.effect.IO
import fs2.kafka.vulcan._

implicit val avroSettings: AvroSettings[IO, Person] =
  AvroSettings {
    SchemaRegistryClientSettings[IO]("http://localhost:8081")
      .withAuth(Auth.Basic("username", "password"))
      .withMaxCacheSize(100)
  }
```

We can use `withIsKey(true)` on `AvroSettings` when the type is used as record key. We can also disable the automatic schema registration when producing using `withAutoRegisterSchemas(false)`. The settings above will create one schema registry client per `Serializer` or `Deserializer` instance.

When both `Codec` and `AvroSettings` are available implicitly for the type, we will be able to create settings (`ConsumerSettings` and `ProducerSettings`) without explicitly specifying the `Serializer` or `Deserializer`, like seen in the following example.

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
  valueDeserializer = avroDeserializer(avroSettings)
).withAutoOffsetReset(AutoOffsetReset.Earliest)
 .withBootstrapServers("localhost")
 .withGroupId("group")

ProducerSettings(
  keySerializer = Serializer[IO, String],
  valueSerializer = avroSerializer(avroSettings)
).withBootstrapServers("localhost")
```

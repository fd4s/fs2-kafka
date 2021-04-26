---
id: modules
title: Modules
---

The following sections describe the additional modules.

## Vulcan

The `@VULCAN_MODULE_NAME@` module provides [Avro](https://avro.apache.org) serialization support using [Vulcan](https://fd4s.github.io/vulcan).

We start by defining the type we want to serialize or deserialize, and create a `Codec`.

```scala mdoc:reset-object
import cats.implicits._
import vulcan.Codec

final case class Person(name: String, age: Option[Int])

implicit val personCodec: Codec[Person] =
  Codec.record(
    name = "Person",
    namespace = "com.example"
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

We use these `AvroSettings` to create an `AvroSchemaRegistryClient`. Here we use `unsafeRunSync`, but this should never be done in production code. Instead the effectful allocation should be composed monadically, as shown below.

```scala mdoc:silent
import fs2.kafka.vulcan.AvroSchemaRegistryClient
import cats.effect.unsafe.implicits.global

val schemaRegistryClient = AvroSchemaRegistryClient(avroSettings).unsafeRunSync()
```

We can then create a `ValueSerializer` and `ValueDeserializer` instance for `Person`.

```scala mdoc:silent
import fs2.kafka.{ValueDeserializer, ValueSerializer}

implicit val personSerializer: ValueSerializer[IO, Person] =
  schemaRegistryClient.valueSerializer[Person]

implicit val personDeserializer: ValueDeserializer[IO, Person] =
  schemaRegistryClient.valueDeserializer[Person]
```

Finally, we can create settings, passing the `Serializer`s and `Deserializer`s implicitly.

```scala mdoc:silent
import fs2.kafka.{AutoOffsetReset, ConsumerSettings, ProducerSettings}

val consumerSettings =
  ConsumerSettings[IO, String, Person]
    .withAutoOffsetReset(AutoOffsetReset.Earliest)
    .withBootstrapServers("localhost:9092")
    .withGroupId("group")

val producerSettings =
  ProducerSettings[IO, String, Person]
    .withBootstrapServers("localhost:9092")
```

If we prefer, we can instead specificy the `Serializer`s and `Deserializer`s explicitly.

```scala mdoc:silent
import fs2.kafka.{Deserializer, Serializer}

ConsumerSettings(
  keyDeserializer = Deserializer[IO, String],
  valueDeserializer = personDeserializer
).withAutoOffsetReset(AutoOffsetReset.Earliest)
 .withBootstrapServers("localhost:9092")
 .withGroupId("group")

ProducerSettings(
  keySerializer = Serializer[IO, String],
  valueSerializer = personSerializer
).withBootstrapServers("localhost:9092")
```

### Composing 


We can then create multiple `Serializer`s and `Deserializer`s using the `AvroSettings`.

```scala mdoc:silent
AvroSchemaRegistryClient(avroSettings).map { schemaRegistryClient =>
  val personSerializer: ValueSerializer[IO, Person] =
    schemaRegistryClient.valueSerializer[Person]

  val personDeserializer: ValueDeserializer[IO, Person] =
    schemaRegistryClient.valueDeserializer[Person]

  val consumerSettings =
    ConsumerSettings(
      keyDeserializer = Deserializer[IO, String],
      valueDeserializer = personDeserializer
    ).withAutoOffsetReset(AutoOffsetReset.Earliest)
    .withBootstrapServers("localhost:9092")
    .withGroupId("group")

 val producerSettings =
  ProducerSettings(
    keySerializer = Serializer[IO, String],
    valueSerializer = personSerializer
  ).withBootstrapServers("localhost:9092")

  (consumerSettings, producerSettings)
}
```

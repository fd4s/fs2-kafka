---
id: modules
title: Modules
---

The following sections describe the additional modules.

## Vulcan

The `@VULCAN_MODULE_NAME@` module provides [Avro](https://avro.apache.org) serialization support using [Vulcan](https://fd4s.github.io/vulcan).

Add it to your project in build.sbt;

```scala
libraryDependencies += "com.github.fd4s" %% "fs2-kafka-vulcan" % fs2KafkaVersion
resolvers += "confluent" at "https://packages.confluent.io/maven/",
```

We start by defining the type we want to serialize or deserialize, and create a `Codec`.

```scala mdoc:reset-object
import cats.syntax.all._
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

We can then create a `Serializer` and `Deserializer` instance for `Person`.

```scala mdoc:silent
import fs2.kafka.{RecordDeserializer, RecordSerializer}
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

## Vulcan testkit munit

The `@VULCAN_TESTKIT_MUNIT_MODULE_NAME@` module provides an [munit](https://scalameta.org/munit/) fixture for testing vulcan 
codecs against a [schema registry](https://docs.confluent.io/platform/current/schema-registry/index.html)

A usage example:

```scala mdoc:reset
import fs2.kafka.vulcan.SchemaRegistryClientSettings
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType
import fs2.kafka.vulcan.testkit.SchemaSuite
import vulcan.Codec

class MySpec extends SchemaSuite {
  val checker = compatibilityChecker(SchemaRegistryClientSettings("https://some-schema-registry:1234"))

  override def munitFixtures = List(checker)

  test("my codec is compatible") {
    val myCodec: Codec[String] = ???

    val compatibility = checker().checkReaderCompatibility(myCodec, "my-schema-subject").unsafeRunSync()

    assertEquals(compatibility.getType(), SchemaCompatibilityType.COMPATIBLE, compatibility.getResult().getIncompatibilities())
  }
}
```
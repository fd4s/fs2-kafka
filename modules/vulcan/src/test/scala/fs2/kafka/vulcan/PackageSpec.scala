package fs2.kafka.vulcan

import java.time.Instant

import cats.implicits._
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.kafka._

import org.scalatest.funspec.AnyFunSpec
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import _root_.vulcan.Codec

final class PackageSpec extends AnyFunSpec {

  describe("AvroSerializer/AvroDeserializer") {
    it("should be able to do roundtrip serialization") {
      (for {
        client <- SchemaRegistryClient(avroSettings)
        serializer = client.valueSerializer[Test]
        test = Test(Instant.now)
        serialized <- serializer.serialize("topic", Headers.empty, test)
        deserializer = client.valueDeserializer[Test]
        deserialized <- deserializer.deserialize("topic", Headers.empty, serialized)
      } yield assert(deserialized == test)).unsafeRunSync()
    }

    it("should be able to do roundtrip serialization using compatible schemas") {
      (for {
        client <- SchemaRegistryClient(avroSettings)
        serializer = client.valueSerializer[Test2]
        test2 = Test2(Instant.now, 42)
        serialized <- serializer.serialize("topic2", Headers.empty, test2)
        deserializer = client.valueDeserializer[Test]
        deserialized <- deserializer.deserialize("topic2", Headers.empty, serialized)
      } yield assert(deserialized == Test(test2.timestamp))).unsafeRunSync()
    }

    it("should error when reader and writer schemas have mismatching logical types") {
      (for {
        client <- SchemaRegistryClient(avroSettings)
        serializer = client.valueSerializer[Long]
        rawLong = 42L
        serialized <- serializer.serialize("topic3", Headers.empty, rawLong)
        deserializer = client.valueDeserializer[Instant]
        deserialized <- deserializer.deserialize("topic3", Headers.empty, serialized).attempt
      } yield assert(deserialized.isLeft)).unsafeRunSync()
    }

    ignore("should not error when reader and writer schemas have mismatching logical types") {
      (for {
        client <- SchemaRegistryClient(avroSettings)
        serializer = client.valueSerializer[Instant]
        serialized <- serializer.serialize("topic3", Headers.empty, Instant.now)
        deserializer = client.valueDeserializer[Instant]
        deserialized <- deserializer.deserialize("topic3", Headers.empty, serialized).attempt
      } yield assert(deserialized.isRight)).unsafeRunSync()
    }
  }

  case class Test(timestamp: Instant)

  object Test {
    implicit val codec: Codec[Test] =
      Codec.record(
        name = "Test",
        namespace = "fs2.kafka.vulcan"
      ) { field =>
        field("name", _.timestamp).map(Test(_))
      }
  }

  case class Test2(timestamp: Instant, number: Int)
  object Test2 {
    implicit val codec: Codec[Test2] =
      Codec.record(
        name = "Test",
        namespace = "fs2.kafka.vulcan"
      ) { field =>
        (
          field("name", _.timestamp),
          field("number", _.number)
        ).mapN(apply)
      }
  }

  val schemaRegistryClient: MockSchemaRegistryClient =
    new MockSchemaRegistryClient()

  val schemaRegistryClientSettings: SchemaRegistryClientSettings[IO] =
    SchemaRegistryClientSettings[IO]("baseUrl")
      .withAuth(Auth.Basic("username", "password"))
      .withMaxCacheSize(100)
      .withCreateSchemaRegistryClient { (_, _, _) =>
        IO.pure(schemaRegistryClient)
      }

  val avroSettings: AvroSettings[IO] =
    AvroSettings(schemaRegistryClientSettings)
}

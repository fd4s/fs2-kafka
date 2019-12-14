package fs2.kafka.vulcan

import cats.implicits._
import cats.effect.IO
import fs2.kafka._
import org.scalatest.funspec.AnyFunSpec
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import _root_.vulcan.Codec

final class PackageSpec extends AnyFunSpec {
  describe("avroSerializer") {
    it("should be available given explicit settings") {
      avroSerializer[Test].using(avroSettings)
    }
  }

  describe("avroDeserializer") {
    it("should be available given explicit settings") {
      avroDeserializer[Test].using(avroSettings)
    }
  }

  describe("avroSerializer/avroDeserializer") {
    it("should be able to do roundtrip serialization") {
      (for {
        serializer <- avroSerializer[Test].using(avroSettings).forValue
        test = Test("test")
        serialized <- serializer.serialize("topic", Headers.empty, test)
        deserializer <- avroDeserializer[Test].using(avroSettings).forValue
        deserialized <- deserializer.deserialize("topic", Headers.empty, serialized)
      } yield assert(deserialized == test)).unsafeRunSync
    }

    it("should be able to do roundtrip serialization using compatible schemas") {
      (for {
        serializer <- avroSerializer[Test2].using(avroSettings).forValue
        test2 = Test2("test", 42)
        serialized <- serializer.serialize("topic", Headers.empty, test2)
        deserializer <- avroDeserializer[Test].using(avroSettings).forValue
        deserialized <- deserializer.deserialize("topic", Headers.empty, serialized)
      } yield assert(deserialized == Test("test"))).unsafeRunSync
    }
  }

  case class Test(name: String)

  object Test {
    implicit val codec: Codec[Test] =
      Codec.record(
        name = "Test",
        namespace = Some("fs2.kafka.vulcan")
      ) { field =>
        field("name", _.name).map(Test(_))
      }
  }

  case class Test2(name: String, number: Int)
  object Test2 {
    implicit val codec: Codec[Test2] =
      Codec.record(
        name = "Test",
        namespace = Some("fs2.kafka.vulcan")
      ) { field =>
        (
          field("name", _.name),
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

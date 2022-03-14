package fs2.kafka.vulcan

import java.time.Instant

import cats.syntax.all._
import cats.effect.IO
import cats.effect.unsafe.implicits.global
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
      (
        avroSerializer[Test].using(avroSettings).forValue,
        avroDeserializer[Test].using(avroSettings).forValue
      ).parTupled
        .use {
          case (serializer, deserializer) =>
            val test = Test("test")

            for {
              serialized <- serializer.serialize("topic", Headers.empty, test)
              deserialized <- deserializer.deserialize("topic", Headers.empty, serialized)
            } yield assert(deserialized == test)
        }
        .unsafeRunSync()
    }

    it("should be able to do roundtrip serialization using compatible schemas") {
      (
        avroSerializer[Test2].using(avroSettings).forValue,
        avroDeserializer[Test].using(avroSettings).forValue
      ).parTupled
        .use {
          case (serializer, deserializer) =>
            val test2 = Test2("test", 42)
            for {

              serialized <- serializer.serialize("topic2", Headers.empty, test2)
              deserialized <- deserializer.deserialize("topic2", Headers.empty, serialized)
            } yield assert(deserialized == Test("test"))
        }
        .unsafeRunSync()
    }

    it("should error when reader and writer schemas have mismatching logical types") {
      (
        avroSerializer[Long].using(avroSettings).forValue,
        avroDeserializer[Instant].using(avroSettings).forValue
      ).parTupled
        .use {
          case (serializer, deserializer) =>
            val rawLong = 42L

            for {
              serialized <- serializer.serialize("topic3", Headers.empty, rawLong)
              deserialized <- deserializer.deserialize("topic3", Headers.empty, serialized).attempt
            } yield assert(deserialized.isLeft)
        }
        .unsafeRunSync()
    }
  }

  case class Test(name: String)

  object Test {
    implicit val codec: Codec[Test] =
      Codec.record(
        name = "Test",
        namespace = "fs2.kafka.vulcan"
      ) { field =>
        field("name", _.name).map(Test(_))
      }
  }

  case class Test2(name: String, number: Int)
  object Test2 {
    implicit val codec: Codec[Test2] =
      Codec.record(
        name = "Test",
        namespace = "fs2.kafka.vulcan"
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

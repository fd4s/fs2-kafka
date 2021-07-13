package fs2.kafka.scalapb

import java.time.Instant
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.google.protobuf.Message
import com.google.protobuf.duration.Duration
import com.google.protobuf.timestamp.Timestamp
import fs2.kafka.{Timestamp => _, _}
import org.scalatest.funspec.AnyFunSpec
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient

import scala.reflect.ClassTag

final class PackageSpec extends AnyFunSpec {
  describe("scalaPBSerializer") {
    it("should be available given explicit settings") {
      protobufSerializer(Timestamp).using(scalaPBSettings)
    }
  }

  describe("scalaPBDeserializer") {
    it("should be available given explicit settings") {
      protobufDeserializer(Timestamp).using(scalaPBSettings)
    }
  }

  describe("scalaPBSerializer/scalaPBDeserializer") {
    it("should be able to do roundtrip serialization") {
      (for {
        serializer <- protobufSerializer(Timestamp).using(scalaPBSettings).forValue
        serialized <- serializer.serialize("topic", Headers.empty, timestamp)
        deserializer <- protobufDeserializer(Timestamp).using(scalaPBSettings).forValue
        deserialized <- deserializer.deserialize("topic", Headers.empty, serialized)
      } yield assert(deserialized == timestamp)).unsafeRunSync()
    }

    it("should be able to do roundtrip serialization using compatible schemas") {
      (for {
        serializer <- protobufSerializer(Duration).using(scalaPBSettings).forValue
        serialized <- serializer.serialize("topic2", Headers.empty, duration)
        deserializer <- protobufDeserializer(Duration).using(scalaPBSettings).forValue
        deserialized <- deserializer.deserialize("topic2", Headers.empty, serialized)
      } yield assert(deserialized == duration)).unsafeRunSync()
    }

  }

  val timestamp: Timestamp = Timestamp(Instant.parse("2021-05-05T00:00:00Z"))
  val duration: Duration = Duration(java.time.Duration.ofHours(1))

  val schemaRegistryClient: MockSchemaRegistryClient =
    new MockSchemaRegistryClient()

  val schemaRegistryClientSettings: SchemaRegistryClientSettings[IO] =
    SchemaRegistryClientSettings[IO]("baseUrl")
      .withAuth(Auth.Basic("username", "password"))
      .withMaxCacheSize(100)
      .withCreateSchemaRegistryClient { (_, _, _) =>
        IO.pure(schemaRegistryClient)
      }

  def scalaPBSettings[JavaProto <: Message: ClassTag]: ProtobufSettings[IO, JavaProto] =
    ProtobufSettings(schemaRegistryClientSettings)
}

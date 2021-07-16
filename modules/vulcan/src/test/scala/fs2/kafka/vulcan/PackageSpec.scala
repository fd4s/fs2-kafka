package fs2.kafka.vulcan

import java.time.Instant
import cats.implicits._
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.kafka._
import org.scalatest.funspec.AnyFunSpec
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import _root_.vulcan.Codec

import java.util.UUID

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

  describe("avroSerializer/avroDeserializer with Java resolution") {
    it("should be able to do roundtrip serialization") {
      (for {
        serializer <- avroSerializer[Test].using(avroSettings).forValue
        test = Test("test")
        serialized <- serializer.serialize("topic", Headers.empty, test)
        deserializer <- avroDeserializer[Test].using(avroSettings).forValue
        deserialized <- deserializer.deserialize("topic", Headers.empty, serialized)
      } yield assert(deserialized == test)).unsafeRunSync()
    }

    it("should be able to do roundtrip serialization using compatible schemas") {
      (for {
        serializer <- avroSerializer[Test2].using(avroSettings).forValue
        test2 = Test2("test", 42)
        serialized <- serializer.serialize("topic2", Headers.empty, test2)
        deserializer <- avroDeserializer[Test].using(avroSettings).forValue
        deserialized <- deserializer.deserialize("topic2", Headers.empty, serialized)
      } yield assert(deserialized == Test("test"))).unsafeRunSync()
    }

    it("will error when schema has top-level logical type") {
      (for {
        serializer <- avroSerializer[Instant].using(avroSettings).forValue
        serialized <- serializer.serialize("topic4", Headers.empty, Instant.now)
        deserializer <- avroDeserializer[Instant].using(avroSettings).forValue
        deserialized <- deserializer.deserialize("topic3", Headers.empty, serialized).attempt
      } yield assert(deserialized.isLeft)).unsafeRunSync()
    }

    it("will not error when record fields have mismatching logical type") {
      (for {
        serializer <- avroSerializer[Test].using(avroSettings).forValue
        uuid = UUID.randomUUID()
        serialized <- serializer.serialize("topic4", Headers.empty, Test(uuid.toString))
        deserializer <- avroDeserializer[TestUuid].using(avroSettings).forValue
        deserialized <- deserializer.deserialize("topic4", Headers.empty, serialized)
      } yield assert(deserialized == TestUuid(uuid))).unsafeRunSync()
    }

    it("will fail to decode top-level Decimal") {
      (for {
        serializer <- avroSerializer(Codec.decimal(4, 2)).using(avroSettings).forValue
        serialized <- serializer.serialize("topic5", Headers.empty, BigDecimal("12.34"))
        deserializer <- avroDeserializer(Codec.decimal(4, 2)).using(avroSettings).forValue
        deserialized <- deserializer.deserialize("topic5", Headers.empty, serialized).attempt
      } yield assert(
        deserialized.swap.exists(
          _.getMessage == "Error decoding BigDecimal: Got unexpected type byte[], expected type ByteBuffer"
        )
      )).unsafeRunSync()
    }

    it("will change value of BigDecimal record field when scales mismatch") {
      (for {
        serializer <- avroSerializer(TestDecimal.codec1).using(avroSettings).forValue
        serialized <- serializer.serialize(
          "topic6",
          Headers.empty,
          TestDecimal(BigDecimal("12.34"))
        )
        deserializer <- avroDeserializer(TestDecimal.codec2).using(avroSettings).forValue
        deserialized <- deserializer.deserialize("topic6", Headers.empty, serialized)
      } yield assert(
        deserialized.value == BigDecimal("1.234")
      )).unsafeRunSync()
    }

    it("supports decoding with aliases") {
      (for {
        serializer <- avroSerializer[Test].using(avroSettings).forValue
        test = Test("test")
        serialized <- serializer.serialize("topic8", Headers.empty, test)
        deserializer <- avroDeserializer[TestWithAlias].using(avroSettings).forValue
        deserialized <- deserializer.deserialize("topic8", Headers.empty, serialized)
      } yield assert(deserialized == TestWithAlias("test"))).unsafeRunSync()
    }
  }
  describe("avroSerializer/avroDeserializer without Java resolution") {
    it("should be able to do roundtrip serialization") {
      (for {
        serializer <- avroSerializer[Test].using(avroSettings).forValue
        test = Test("test")
        serialized <- serializer.serialize("topic", Headers.empty, test)
        deserializer <- avroDeserializer[Test].using(avroSettings, false).forValue
        deserialized <- deserializer.deserialize("topic", Headers.empty, serialized)
      } yield assert(deserialized == test)).unsafeRunSync()
    }

    it("should be able to do roundtrip serialization using compatible schemas") {
      (for {
        serializer <- avroSerializer[Test2].using(avroSettings).forValue
        test2 = Test2("test", 42)
        serialized <- serializer.serialize("topic2", Headers.empty, test2)
        deserializer <- avroDeserializer[Test].using(avroSettings, false).forValue
        deserialized <- deserializer.deserialize("topic2", Headers.empty, serialized)
      } yield assert(deserialized == Test("test"))).unsafeRunSync()
    }

    it("will error when schema has top-level logical type") {
      (for {
        serializer <- avroSerializer[Instant].using(avroSettings).forValue
        serialized <- serializer.serialize("topic4", Headers.empty, Instant.now)
        deserializer <- avroDeserializer[Instant].using(avroSettings, false).forValue
        deserialized <- deserializer.deserialize("topic3", Headers.empty, serialized).attempt
      } yield assert(deserialized.isLeft)).unsafeRunSync()
    }

    it("will error when record fields have mismatching logical type") {
      (for {
        serializer <- avroSerializer[Test].using(avroSettings).forValue
        uuid = UUID.randomUUID()
        serialized <- serializer.serialize("topic4", Headers.empty, Test(uuid.toString))
        deserializer <- avroDeserializer[TestUuid].using(avroSettings, false).forValue
        deserialized <- deserializer.deserialize("topic4", Headers.empty, serialized).attempt
      } yield assert(deserialized.isLeft)).unsafeRunSync()
    }

    it("will fail to decode top-level Decimal") {
      (for {
        serializer <- avroSerializer(Codec.decimal(4, 2)).using(avroSettings).forValue
        serialized <- serializer.serialize("topic5", Headers.empty, BigDecimal("12.34"))
        deserializer <- avroDeserializer(Codec.decimal(4, 2)).using(avroSettings, false).forValue
        deserialized <- deserializer.deserialize("topic5", Headers.empty, serialized).attempt
      } yield assert(
        deserialized.swap.exists(
          _.getMessage == "Error decoding BigDecimal: Got unexpected type byte[], expected type ByteBuffer"
        )
      )).unsafeRunSync()
    }

    it("will not change value of BigDecimal record field when scales mismatch") {
      (for {
        serializer <- avroSerializer(TestDecimal.codec1).using(avroSettings).forValue
        serialized <- serializer.serialize(
          "topic6",
          Headers.empty,
          TestDecimal(BigDecimal("12.34"))
        )
        deserializer <- avroDeserializer(TestDecimal.codec2).using(avroSettings, false).forValue
        deserialized <- deserializer.deserialize("topic6", Headers.empty, serialized)
      } yield assert(
        deserialized.value == BigDecimal("12.34")
      )).unsafeRunSync()
    }

    it("does not support decoding with aliases") {
      (for {
        serializer <- avroSerializer[Test].using(avroSettings).forValue
        test = Test("test")
        serialized <- serializer.serialize("topic8", Headers.empty, test)
        deserializer <- avroDeserializer[TestWithAlias].using(avroSettings, false).forValue
        deserialized <- deserializer.deserialize("topic8", Headers.empty, serialized).attempt
      } yield assert(deserialized.isLeft)).unsafeRunSync()
    }
  }

  case class TestDecimal(value: BigDecimal)
  object TestDecimal {
    val codec1: Codec[TestDecimal] = Codec.record("TestDecimal", "fs2.kafka.vulcan") { field =>
      field("value", _.value)(Codec.decimal(4, 2)).map(apply)
    }
    val codec2: Codec[TestDecimal] = Codec.record("TestDecimal", "fs2.kafka.vulcan") { field =>
      field("value", _.value)(Codec.decimal(4, 3)).map(apply)
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

  case class TestWithAlias(nom: String)
  object TestWithAlias {
    implicit val codec: Codec[TestWithAlias] =
      Codec.record(
        name = "Test",
        namespace = "fs2.kafka.vulcan"
      ) { field =>
        field("nom", _.nom, aliases = Seq("name")).map(apply)
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

  case class TestUuid(name: UUID)
  object TestUuid {
    implicit val codec: Codec[TestUuid] =
      Codec.record("Test", "fs2.kafka.vulcan") { field =>
        field("name", _.name).map(apply)
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

package fs2.kafka.vulcan

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.kafka.schemaregistry.client.SchemaRegistryClient
import fs2.kafka.Headers
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import org.scalatest.funspec.AnyFunSpec
import vulcan.{AvroError, Codec}

final class AvroDeserializerSpec extends AnyFunSpec {

  val avroSettings: AvroSettings[IO] =
    AvroSettings(SchemaRegistryClient.fromJava[IO](new MockSchemaRegistryClient()))

  describe("AvroDeserializer") {
    it("can create a deserializer") {
      val deserializer =
        AvroDeserializer[Int].using(avroSettings)

      assert(deserializer.forKey.attempt.unsafeRunSync().isRight)
      assert(deserializer.forValue.attempt.unsafeRunSync().isRight)
    }

    it("raises schema errors") {
      val codec: Codec[Int] =
        Codec.instance(
          Left(AvroError("error")),
          _ => Left(AvroError("encode")),
          (_, _) => Left(AvroError("decode"))
        )

      val deserializer =
        avroDeserializer(codec).using(avroSettings)

      assert(deserializer.forKey.attempt.unsafeRunSync().isLeft)
      assert(deserializer.forValue.attempt.unsafeRunSync().isLeft)
    }

    it("raises IllegalArgumentException if the data is null") {
      val deserializer = AvroDeserializer[String].using(avroSettings)
      intercept[IllegalArgumentException] {
        deserializer.forKey.flatMap(_.deserialize("foo", Headers.empty, null)).unsafeRunSync()
      }
    }

    it("toString") {
      assert {
        avroDeserializer[Int].toString() startsWith "AvroDeserializer$"
      }
    }
  }
}

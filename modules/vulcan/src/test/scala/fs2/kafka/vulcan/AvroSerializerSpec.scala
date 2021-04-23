package fs2.kafka.vulcan

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import org.scalatest.funspec.AnyFunSpec
import vulcan.{AvroError, Codec}

final class AvroSerializerSpec extends AnyFunSpec {
  describe("AvroSerializer") {
    it("can create a serializer") {
      val keySerializer =
        AvroSerializer[Int].forKey(avroSettings)

      val valueSerializer =
        AvroSerializer[Int].forValue(avroSettings)

      assert(keySerializer.forKey.attempt.unsafeRunSync().isRight)
      assert(valueSerializer.forValue.attempt.unsafeRunSync().isRight)
    }

    it("raises schema errors") {
      val codec: Codec[Int] =
        Codec.instance(
          Left(AvroError("error")),
          _ => Left(AvroError("encode")),
          (_, _) => Left(AvroError("decode"))
        )

      val keySerializer =
        AvroSerializer(codec).forKey(avroSettings)

      val valueSerializer =
        AvroSerializer(codec).forValue(avroSettings)

      assert(keySerializer.forKey.attempt.unsafeRunSync().isRight)
      assert(valueSerializer.forValue.attempt.unsafeRunSync().isRight)
    }

    it("toString") {
      assert {
        AvroSerializer[Int].toString() startsWith "AvroSerializer$"
      }
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

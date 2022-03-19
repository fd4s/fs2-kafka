package fs2.kafka.vulcan

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import org.scalatest.funspec.AnyFunSpec
import vulcan.{AvroError, Codec}

final class AvroSerializerSpec extends AnyFunSpec {
  describe("AvroSerializer") {
    it("can create a serializer") {
      assert(AvroSerializer[Int].forKey(avroSettings).use(IO.pure).attempt.unsafeRunSync().isRight)
      assert(AvroSerializer[Int].forValue(avroSettings).use(IO.pure).attempt.unsafeRunSync().isRight)
    }

    it("raises schema errors") {
      val codec: Codec[Int] =
        Codec.instance(
          Left(AvroError("error")),
          _ => Left(AvroError("encode")),
          (_, _) => Left(AvroError("decode"))
        )

      assert(avroSerializer(codec).forKey(avroSettings).use(IO.pure).attempt.unsafeRunSync().isRight)
      assert(avroSerializer(codec).forValue(avroSettings).use(IO.pure).attempt.unsafeRunSync().isRight)
    }

    it("toString") {
      assert {
        avroSerializer[Int].toString() startsWith "AvroSerializer$"
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

package fs2.kafka.vulcan

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import org.scalatest.funspec.AnyFunSpec

final class AvroDeserializerSpec extends AnyFunSpec {
  describe("AvroDeserializer") {
    it("can create a deserializer") {
      val src: AvroSchemaRegistryClient[IO] = AvroSchemaRegistryClient(avroSettings).unsafeRunSync()

      src.keyDeserializer[Int]

      src.valueDeserializer[Int]
    }
//
//    it("raises schema errors") {
//      val codec: Codec[Int] =
//        Codec.instance(
//          Left(AvroError("error")),
//          _ => Left(AvroError("encode")),
//          (_, _) => Left(AvroError("decode"))
//        )
//
//      val keyDeserializer =
//        AvroDeserializer(codec).forKey(avroSettings)
//
//      val valueDeserializer =
//        AvroDeserializer(codec).forValue(avroSettings)
//
//      assert(keyDeserializer.forKey.attempt.unsafeRunSync().isLeft)
//      assert(valueDeserializer.forValue.attempt.unsafeRunSync().isLeft)
//    }
//
//    it("toString") {
//      assert {
//        AvroDeserializer[Int].toString() startsWith "AvroDeserializer$"
//      }
//    }
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

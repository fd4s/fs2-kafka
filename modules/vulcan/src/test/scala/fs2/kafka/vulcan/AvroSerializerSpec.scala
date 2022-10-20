package fs2.kafka.vulcan

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.kafka.schemaregistry.client.SchemaRegistryClient
import fs2.kafka.Headers
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import org.scalatest.funspec.AnyFunSpec
import vulcan.{AvroError, Codec}

final class AvroSerializerSpec extends AnyFunSpec {

  val avroSettings: AvroSettings[IO] =
    AvroSettings(SchemaRegistryClient.fromJava[IO](new MockSchemaRegistryClient()))

  describe("AvroSerializer") {
    it("can create a serializer") {
      val serializer =
        AvroSerializer[Int].using(avroSettings)

      assert(serializer.forKey.attempt.unsafeRunSync().isRight)
      assert(serializer.forValue.attempt.unsafeRunSync().isRight)
    }

    it("auto-registers union schemas") {
      (avroSerializer[Either[Int, Boolean]]
        .using(avroSettings)
        .forValue
        .flatMap(
          _.serialize(
            "test-union-topic",
            Headers.empty,
            Right(true)
          )
        ))
        .unsafeRunSync()

      avroSettings.schemaRegistryClient
        .flatMap(_.getLatestSchemaMetadata("test-union-topic-value"))
        .map(
          latestSchemaMetadata =>
            assert(
              latestSchemaMetadata.getSchema === """["int","boolean"]"""
            )
        )
        .unsafeRunSync()
    }

    it("raises schema errors") {
      val codec: Codec[Int] =
        Codec.instance(
          Left(AvroError("error")),
          _ => Left(AvroError("encode")),
          (_, _) => Left(AvroError("decode"))
        )

      val serializer =
        avroSerializer(codec).using(avroSettings)

      assert(serializer.forKey.attempt.unsafeRunSync().isRight)
      assert(serializer.forValue.attempt.unsafeRunSync().isRight)
    }

    it("toString") {
      assert {
        avroSerializer[Int].toString() startsWith "AvroSerializer$"
      }
    }
  }
}

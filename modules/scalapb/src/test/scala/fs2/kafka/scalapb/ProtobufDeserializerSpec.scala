package fs2.kafka.scalapb

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.google.protobuf.Message
import com.google.protobuf.wrappers.Int32Value
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import org.scalatest.funspec.AnyFunSpec

import scala.reflect.ClassTag

final class ProtobufDeserializerSpec extends AnyFunSpec {
  describe("ProtobufDeserializer") {
    it("can create a deserializer") {
      val deserializer =
        ProtobufDeserializer(Int32Value).using(scalaPBSettings)

      assert(deserializer.forKey.attempt.unsafeRunSync().isRight)
      assert(deserializer.forValue.attempt.unsafeRunSync().isRight)
    }

    it("raises schema errors") {

      val deserializer =
        protobufDeserializer(Int32Value).using(scalaPBSettings[com.google.protobuf.Int32Value].withCreateProtobufDeserializer((_, _, _) => IO.raiseError(new RuntimeException)))

      assert(deserializer.forKey.attempt.unsafeRunSync().isLeft)
      assert(deserializer.forValue.attempt.unsafeRunSync().isLeft)
    }

    it("toString") {
      assert {
        protobufDeserializer(Int32Value).toString() startsWith "ProtobufDeserializer$"
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

  def scalaPBSettings[JavaProto <: Message : ClassTag]: ProtobufSettings[IO, JavaProto] =
    ProtobufSettings(schemaRegistryClientSettings)
}

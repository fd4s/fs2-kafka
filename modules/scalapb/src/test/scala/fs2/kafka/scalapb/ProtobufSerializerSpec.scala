package fs2.kafka.scalapb

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.google.protobuf.Message
import com.google.protobuf.wrappers.Int32Value
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import org.scalatest.funspec.AnyFunSpec

import scala.reflect.ClassTag

final class ProtobufSerializerSpec extends AnyFunSpec {
  describe("ProtobufSerializer") {
    it("can create a serializer") {
      val serializer =
        ProtobufSerializer(Int32Value).using(scalaPBSettings)

      assert(serializer.forKey.attempt.unsafeRunSync().isRight)
      assert(serializer.forValue.attempt.unsafeRunSync().isRight)
    }

    it("raises schema errors") {
      val serializer =
        protobufSerializer(Int32Value).using(scalaPBSettings)

      assert(serializer.forKey.attempt.unsafeRunSync().isRight)
      assert(serializer.forValue.attempt.unsafeRunSync().isRight)
    }

    it("toString") {
      assert {
        protobufSerializer(Int32Value).toString() startsWith "ProtobufSerializer$"
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

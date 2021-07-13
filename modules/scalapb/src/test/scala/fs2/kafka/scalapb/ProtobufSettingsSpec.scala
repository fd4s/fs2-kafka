package fs2.kafka.scalapb

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.google.protobuf.Message
import org.scalatest.funspec.AnyFunSpec
import org.scalatestplus.scalacheck._

import scala.reflect.ClassTag

final class ProtobufSettingsSpec extends AnyFunSpec with ScalaCheckPropertyChecks {
  describe("ProtobufSettings") {
    it("should provide withAutoRegisterSchemas") {
      forAll { (value: Boolean) =>
        assert {
          settings
            .withAutoRegisterSchemas(value)
            .properties
            .get("auto.register.schemas")
            .contains(value.toString)
        }
      }
    }

    it("should provide withKeySubjectNameStrategy") {
      forAll { (value: String) =>
        assert {
          settings
            .withKeySubjectNameStrategy(value)
            .properties
            .get("key.subject.name.strategy")
            .contains(value)
        }
      }
    }

    it("should provide withValueSubjectNameStrategy") {
      forAll { (value: String) =>
        assert {
          settings
            .withValueSubjectNameStrategy(value)
            .properties
            .get("value.subject.name.strategy")
            .contains(value)
        }
      }
    }

    it("should provide withProperty") {
      forAll { (key: String, value: String) =>
        settings
          .withProperty(key, value)
          .properties
          .get(key)
          .contains(value)
      }
    }

    it("should provide withProperties") {
      forAll { (key: String, value: String) =>
        settings
          .withProperties(key -> value)
          .properties
          .get(key)
          .contains(value)
      }
    }

    it("should provide withProperties(Map)") {
      forAll { (key: String, value: String) =>
        settings
          .withProperties(Map(key -> value))
          .properties
          .get(key)
          .contains(value)
      }
    }

    it("should provide withCreateProtobufDeserializer") {
      assert {
        settings
          .withCreateProtobufDeserializer((_, _, _) => IO.raiseError(new RuntimeException))
          .createProtobufDeserializer(isKey = false)
          .attempt
          .unsafeRunSync()
          .isLeft
      }
    }

    it("should provide withCreateProtobufSerializer") {
      assert {
        settings
          .withCreateProtobufSerializer((_, _, _) => IO.raiseError(new RuntimeException))
          .createProtobufSerializer(isKey = false)
          .attempt
          .unsafeRunSync()
          .isLeft
      }
    }

    it("should provide toString") {
      assert {
        settings.toString.startsWith("ProtobufSettings$")
      }
    }
  }

  def settings[JavaProto <: Message : ClassTag]: ProtobufSettings[IO, JavaProto] =
    ProtobufSettings(SchemaRegistryClientSettings[IO]("baseUrl"))

  def settingsWithClient[JavaProto <: Message : ClassTag]: ProtobufSettings[IO, JavaProto] =
    ProtobufSettings(null: SchemaRegistryClient)
}

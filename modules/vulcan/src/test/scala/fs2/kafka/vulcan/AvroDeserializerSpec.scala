package fs2.kafka.vulcan

import cats.effect.IO
import fs2.kafka.Headers
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import org.scalatest.funspec.AnyFunSpec
import vulcan.{AvroError, Codec}

final class AvroDeserializerSpec extends AnyFunSpec {
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

    it("toString") {
      assert {
        avroDeserializer[Int].toString() startsWith "AvroDeserializer$"
      }
    }

    describe("topics") {
      it("can create a deserializer from multiple codecs") {

        val deserializer =
          AvroDeserializer.topics(avroSettings)(
            ("someTopic", Codec[Int]),
            ("anotherTopic", Codec[Int])
          )

        assert(deserializer.forKey.attempt.unsafeRunSync().isRight)
        assert(deserializer.forValue.attempt.unsafeRunSync().isRight)
      }

      it("can correctly differentiate between incoming topics") {

        sealed trait SomeTrait
        case class SomeCaseClass(someField: String) extends SomeTrait
        case class AnotherCaseClass(anotherField: Int) extends SomeTrait

        implicit val someCodec = Codec[String].imap(SomeCaseClass)(_.someField)
        implicit val anotherCodec = Codec[Int].imap(AnotherCaseClass)(_.anotherField)

        val deserializer =
          AvroDeserializer.topics[IO, SomeTrait](avroSettings)(
            ("someTopic", someCodec),
            ("anotherTopic", anotherCodec)
          ).forValue.unsafeRunSync()

        val someTopicResult = deserializer.deserialize("someTopic", Headers.empty, encode("someTopic", SomeCaseClass("someValue"))).unsafeRunSync()
        val anotherTopicResult =  deserializer.deserialize("anotherTopic", Headers.empty, encode("anotherTopic", AnotherCaseClass(123))).unsafeRunSync()

        someTopicResult match {
          case SomeCaseClass(s) => assert(s == "someValue")
          case _ => fail()
        }

        anotherTopicResult match {
          case AnotherCaseClass(i) => assert(i == 123)
          case _ => fail()
        }
      }
    }
  }

  def encode[A: Codec](topic: String, value: A) = AvroSerializer[A].using(avroSettings).forValue.flatMap(_.serialize(topic, Headers.empty, value)).unsafeRunSync()

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

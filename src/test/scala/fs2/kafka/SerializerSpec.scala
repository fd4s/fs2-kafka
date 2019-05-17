package fs2.kafka

import cats._
import cats.effect.IO
import cats.laws.discipline._
import java.nio.charset.StandardCharsets
import java.util.UUID
import org.apache.kafka.common.utils.Bytes
import org.scalacheck.Arbitrary
import org.scalatest._
import scala.util.Try

final class SerializerSpec extends BaseCatsSpec {
  type IdSerializer[A] = Serializer[Id, A]

  implicit def idSerializerApplicative: Applicative[Id] =
    catsInstancesForId

  implicit def idSerializerArbitrary: Arbitrary[IdSerializer[String]] =
    arbSerializerString

  checkAll(
    "Serializer[Id, ?]",
    ContravariantTests[IdSerializer].contravariant[String, String, String]
  )

  test("Serializer#mapBytes") {
    val serializer =
      Serializer
        .identity[Id]
        .mapBytes(Array(0.toByte) ++ _)

    forAll { (topic: String, bytes: Array[Byte]) =>
      serializer.serialize(topic, Headers.empty, bytes) shouldBe (Array(0.toByte) ++ bytes)
    }
  }

  test("Serializer#bytes") {
    val serializer =
      Serializer.bytes[Id]

    forAll { (topic: String, headers: Headers, bytes: Array[Byte]) =>
      val serialized = serializer.serialize(topic, headers, new Bytes(bytes))
      serialized shouldBe bytes
    }
  }

  test("Serializer#const") {
    val serializer =
      Serializer.const[Id, Int](Array())

    forAll { (topic: String, i: Int) =>
      serializer.serialize(topic, Headers.empty, i).isEmpty shouldBe true
    }
  }

  test("Serializer#defer") {
    val serializer =
      Serializer
        .delegate[Eval, Int](
          new KafkaSerializer[Int] {
            override def close(): Unit = ()
            override def configure(props: java.util.Map[String, _], isKey: Boolean): Unit = ()
            override def serialize(topic: String, int: Int): Array[Byte] =
              throw new RuntimeException
          }
        )
        .defer

    forAll { (topic: String, headers: Headers, int: Int) =>
      val serialized = serializer.serialize(topic, headers, int)
      Try(serialized.value).isFailure shouldBe true
    }
  }

  test("Serializer#fail") {
    val serializer =
      Serializer.fail[IO, Int](new RuntimeException)

    forAll { (topic: String, headers: Headers, i: Int) =>
      val serialized = serializer.serialize(topic, headers, i)
      assert(serialized.attempt.unsafeRunSync.isLeft)
    }
  }

  test("Serializer#suspend") {
    val serializer =
      Serializer
        .delegate[IO, Int](
          new KafkaSerializer[Int] {
            override def close(): Unit = ()
            override def configure(props: java.util.Map[String, _], isKey: Boolean): Unit = ()
            override def serialize(topic: String, int: Int): Array[Byte] =
              throw new RuntimeException
          }
        )
        .suspend

    forAll { (topic: String, headers: Headers, int: Int) =>
      val serialized = serializer.serialize(topic, headers, int)
      serialized.attempt.unsafeRunSync shouldBe a[Left[_, _]]
    }
  }

  test("Serializer#headers") {
    val serializer =
      Serializer.headers { headers =>
        headers("format").map(_.as[String]) match {
          case Some("int") => Serializer[Id, Int]
          case _           => Serializer[Id, String].contramap[Int](_.toString)
        }
      }

    forAll { (topic: String, i: Int) =>
      val headers = Header("format", "int").headers
      val serialized = serializer.serialize(topic, headers, i)
      val expected = Serializer[Id, Int].serialize(topic, Headers.empty, i)
      serialized shouldBe expected
    }

    forAll { (topic: String, i: Int) =>
      val serialized = serializer.serialize(topic, Headers.empty, i)
      val expected =
        Serializer[Id, String].contramap[Int](_.toString).serialize(topic, Headers.empty, i)
      serialized shouldBe expected
    }
  }

  test("Serializer#topic") {
    val serializer =
      Serializer.topic {
        case "topic" => Serializer[Id, Int]
        case _       => Serializer[Id, String].contramap[Int](_.toString)
      }

    forAll { i: Int =>
      val serialized = serializer.serialize("topic", Headers.empty, i)
      val expected = Serializer[Id, Int].serialize("topic", Headers.empty, i)
      serialized shouldBe expected
    }

    forAll { (topic: String, i: Int) =>
      whenever(topic != "topic") {
        val serialized = serializer.serialize(topic, Headers.empty, i)
        val expected =
          Serializer[Id, String].contramap[Int](_.toString).serialize(topic, Headers.empty, i)
        serialized shouldBe expected
      }
    }
  }

  test("Serializer#asNull") {
    val serializer =
      Serializer.asNull[Id, Int]

    forAll { i: Int =>
      val serialized = serializer.serialize("topic", Headers.empty, i)
      serialized shouldBe null
    }
  }

  test("Serializer#empty") {
    val serializer =
      Serializer.empty[Id, Int]

    forAll { i: Int =>
      val serialized = serializer.serialize("topic", Headers.empty, i)
      serialized shouldBe empty
    }
  }

  test("Serializer#option") {
    val serializer =
      Serializer[Id, Option[String]]

    serializer.serialize("topic", Headers.empty, None) shouldBe null

    forAll { s: String =>
      serializer.serialize("topic", Headers.empty, Some(s)) shouldBe
        Serializer[Id, String].serialize("topic", Headers.empty, s)
    }
  }

  test("Serializer#unit") {
    Serializer[Id, Unit].serialize("topic", Headers.empty, ()) shouldBe null
  }

  test("Serializer#toString") {
    assert(Serializer[Id, Int].toString startsWith "Serializer$")
  }

  def roundtrip[A: Arbitrary: Eq](
    serializer: Serializer[IO, A],
    deserializer: Deserializer[IO, A]
  ): Assertion = forAll { (topic: String, headers: Headers, a: A) =>
    val serialized = serializer.serialize(topic, headers, a).unsafeRunSync
    val deserialized = deserializer.deserialize(topic, headers, serialized).unsafeRunSync
    assert(deserialized === a)
  }

  def roundtripAttempt[A: Arbitrary: Eq](
    serializer: Serializer[IO, A],
    deserializer: Deserializer[IO, A]
  ): Assertion = forAll { (topic: String, headers: Headers, a: A) =>
    val serialized = serializer.serialize(topic, headers, a).unsafeRunSync
    val deserialized = deserializer.deserialize(topic, headers, serialized)
    assert(deserialized.attempt.unsafeRunSync.toOption === Option(a))
  }

  test("Serializer#string") {
    roundtrip(
      Serializer.string[IO](StandardCharsets.UTF_8),
      Deserializer.string(StandardCharsets.UTF_8)
    )
  }

  test("Serializer#uuid") {
    roundtripAttempt(
      Serializer.uuid[IO](StandardCharsets.UTF_8),
      Deserializer.uuid(StandardCharsets.UTF_8)
    )
  }

  test("Serializer#uuid.default") {
    roundtripAttempt(
      Serializer[IO, UUID],
      Deserializer.uuid
    )
  }

  test("Serializer#double") {
    roundtripAttempt(
      Serializer.double[IO],
      Deserializer.double
    )
  }

  test("Serializer#float") {
    roundtripAttempt(
      Serializer.float[IO],
      Deserializer.float
    )
  }

  test("Serializer#int") {
    roundtripAttempt(
      Serializer.int[IO],
      Deserializer.int
    )
  }

  test("Serializer#long") {
    roundtripAttempt(
      Serializer.long[IO],
      Deserializer.long
    )
  }

  test("Serializer#short") {
    roundtripAttempt(
      Serializer.short[IO],
      Deserializer.short
    )
  }
}

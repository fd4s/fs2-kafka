package fs2.kafka

import cats.Eq
import cats.laws.discipline._
import java.nio.charset.StandardCharsets
import org.scalacheck.Arbitrary
import org.scalatest._
import scala.collection.JavaConverters._

final class SerializerSpec extends BaseCatsSpec {
  checkAll("Serializer", ContravariantTests[Serializer].contravariant[String, String, String])

  // Always only returns `Unit`. Invoked to include in coverage.
  test("Serializer#configure") { Serializer[Int].configure(Map.empty.asJava, true) }
  test("Serializer#close") { Serializer[Int].close() }

  test("Serializer#mapBytes") {
    val serializer =
      Serializer.identity
        .mapBytes(Array(0.toByte) ++ _)

    forAll { (topic: String, bytes: Array[Byte]) =>
      serializer.serialize(topic, bytes) shouldBe (Array(0.toByte) ++ bytes)
    }
  }

  test("Serializer#const") {
    val serializer =
      Serializer.const[Int](Array())

    forAll { (topic: String, i: Int) =>
      serializer.serialize(topic, i).isEmpty shouldBe true
    }
  }

  test("Serializer#headers") {
    val serializer =
      Serializer.headers { headers =>
        headers("format").map(_.as[String]) match {
          case Some("int") => Serializer[Int]
          case _           => Serializer[String].contramap[Int](_.toString)
        }
      }

    forAll { (topic: String, i: Int) =>
      val headers = Header("format", "int").headers
      val serialized = serializer.serialize(topic, headers, i)
      val expected = Serializer[Int].serialize(topic, i)
      serialized shouldBe expected
    }

    forAll { (topic: String, i: Int) =>
      val serialized = serializer.serialize(topic, i)
      val expected = Serializer[String].contramap[Int](_.toString).serialize(topic, i)
      serialized shouldBe expected
    }
  }

  test("Serializer#topic") {
    val serializer =
      Serializer.topic {
        case "topic" => Serializer[Int]
        case _       => Serializer[String].contramap[Int](_.toString)
      }

    forAll { i: Int =>
      val serialized = serializer.serialize("topic", i)
      val expected = Serializer[Int].serialize("topic", i)
      serialized shouldBe expected
    }

    forAll { (topic: String, i: Int) =>
      whenever(topic != "topic") {
        val serialized = serializer.serialize(topic, i)
        val expected = Serializer[String].contramap[Int](_.toString).serialize(topic, i)
        serialized shouldBe expected
      }
    }
  }

  test("Serializer#asNull") {
    val serializer =
      Serializer.asNull[Int]

    forAll { i: Int =>
      val serialized = serializer.serialize("topic", i)
      serialized shouldBe null
    }
  }

  test("Serializer#empty") {
    val serializer =
      Serializer.empty[Int]

    forAll { i: Int =>
      val serialized = serializer.serialize("topic", i)
      serialized shouldBe empty
    }
  }

  test("Serializer#option") {
    val serializer =
      Serializer[Option[String]]

    serializer.serialize("topic", None) shouldBe null

    forAll { s: String =>
      serializer.serialize("topic", Some(s)) shouldBe s.getBytes
    }
  }

  test("Serializer#unit") {
    Serializer[Unit].serialize("topic", ()) shouldBe null
  }

  test("Serializer#toString") {
    assert(Serializer[Int].toString startsWith "Serializer$")
  }

  def roundtrip[A: Arbitrary: Eq](
    serializer: Serializer[A],
    deserializer: Deserializer[A]
  ): Assertion = forAll { (topic: String, headers: Headers, a: A) =>
    val serialized = serializer.serialize(topic, headers, a)
    val deserialized = deserializer.deserialize(topic, headers, serialized)
    assert(deserialized === a)
  }

  def roundtripAttempt[A: Arbitrary: Eq](
    serializer: Serializer[A],
    deserializer: Deserializer.Attempt[A]
  ): Assertion = forAll { (topic: String, headers: Headers, a: A) =>
    val serialized = serializer.serialize(topic, headers, a)
    val deserialized = deserializer.deserialize(topic, headers, serialized)
    assert(deserialized.toOption === Option(a))
  }

  test("Serializer#string") {
    roundtrip(
      Serializer.string(StandardCharsets.UTF_8),
      Deserializer.string(StandardCharsets.UTF_8)
    )
  }

  test("Serializer#uuid") {
    roundtripAttempt(
      Serializer.uuid(StandardCharsets.UTF_8),
      Deserializer.uuid(StandardCharsets.UTF_8)
    )
  }

  test("Serializer#double") {
    roundtripAttempt(
      Serializer.double,
      Deserializer.double
    )
  }

  test("Serializer#float") {
    roundtripAttempt(
      Serializer.float,
      Deserializer.float
    )
  }

  test("Serializer#int") {
    roundtripAttempt(
      Serializer.int,
      Deserializer.int
    )
  }

  test("Serializer#long") {
    roundtripAttempt(
      Serializer.long,
      Deserializer.long
    )
  }

  test("Serializer#short") {
    roundtripAttempt(
      Serializer.short,
      Deserializer.short
    )
  }
}

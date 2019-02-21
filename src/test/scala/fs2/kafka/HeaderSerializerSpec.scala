package fs2.kafka

import cats._
import cats.laws.discipline._
import java.nio.charset._
import org.scalacheck._
import org.scalatest._

final class HeaderSerializerSpec extends BaseCatsSpec {
  checkAll(
    "HeaderSerializer",
    ContravariantTests[HeaderSerializer].contravariant[String, String, String]
  )

  test("HeaderSerializer#mapBytes") {
    val serializer =
      HeaderSerializer.identity
        .mapBytes(Array(0.toByte) ++ _)

    forAll { bytes: Array[Byte] =>
      serializer.serialize(bytes) shouldBe (Array(0.toByte) ++ bytes)
    }
  }

  test("HeaderSerializer#const") {
    val serializer =
      HeaderSerializer.const[Int](Array())

    forAll { i: Int =>
      serializer.serialize(i).isEmpty shouldBe true
    }
  }

  test("HeaderSerializer#toString") {
    assert(HeaderSerializer[Int].toString startsWith "HeaderSerializer$")
  }

  def roundtrip[A: Arbitrary: Eq](
    serializer: HeaderSerializer[A],
    deserializer: HeaderDeserializer[A]
  ): Assertion = forAll { a: A =>
    val serialized = serializer.serialize(a)
    val deserialized = deserializer.deserialize(serialized)
    assert(deserialized === a)
  }

  def roundtripAttempt[A: Arbitrary: Eq](
    serializer: HeaderSerializer[A],
    deserializer: HeaderDeserializer[Either[Throwable, A]]
  ): Assertion = forAll { a: A =>
    val serialized = serializer.serialize(a)
    val deserialized = deserializer.deserialize(serialized)
    assert(deserialized.toOption === Option(a))
  }

  test("HeaderSerializer#string") {
    roundtrip(
      HeaderSerializer.string(StandardCharsets.UTF_8),
      HeaderDeserializer.string(StandardCharsets.UTF_8)
    )
  }

  test("Serializer#uuid") {
    roundtripAttempt(
      HeaderSerializer.uuid(StandardCharsets.UTF_8),
      HeaderDeserializer.uuid(StandardCharsets.UTF_8)
    )
  }

  test("HeaderSerializer#double") {
    roundtripAttempt(
      HeaderSerializer.double,
      HeaderDeserializer.double
    )
  }

  test("HeaderSerializer#float") {
    roundtripAttempt(
      HeaderSerializer.float,
      HeaderDeserializer.float
    )
  }

  test("HeaderSerializer#int") {
    roundtripAttempt(
      HeaderSerializer.int,
      HeaderDeserializer.int
    )
  }

  test("HeaderSerializer#long") {
    roundtripAttempt(
      HeaderSerializer.long,
      HeaderDeserializer.long
    )
  }

  test("HeaderSerializer#short") {
    roundtripAttempt(
      HeaderSerializer.short,
      HeaderDeserializer.short
    )
  }
}

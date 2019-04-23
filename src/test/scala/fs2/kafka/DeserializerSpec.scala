package fs2.kafka

import cats._
import cats.effect.IO
import cats.laws.discipline._
import org.scalacheck.Arbitrary

final class DeserializerSpec extends BaseCatsSpec {
  type IdDeserializer[A] = Deserializer[Id, A]

  implicit def idApplicative: Applicative[Id] =
    catsInstancesForId

  implicit def idDeserializerArbitrary: Arbitrary[IdDeserializer[String]] =
    arbDeserializerString

  implicit def idDeserializerCombine: Arbitrary[IdDeserializer[String => String]] =
    arbDeserializerCombine[Id, String, String]

  implicit def idDeserializerIsomorphisms: SemigroupalTests.Isomorphisms[IdDeserializer] =
    SemigroupalTests.Isomorphisms.invariant[IdDeserializer]

  checkAll("Deserializer[Id, ?]", MonadTests[IdDeserializer].monad[String, String, String])

  test("Deserializer#attempt") {
    forAll { (topic: String, headers: Headers, i: Int) =>
      val bytes = Serializer[IO, Int].serialize(topic, headers, i).unsafeRunSync

      val deserialized = Deserializer[IO, Int].deserialize(topic, headers, bytes)
      assert(deserialized.attempt.unsafeRunSync.isRight)

      val deserializedTwice = Deserializer[IO, Int].deserialize(topic, headers, bytes ++ bytes)
      assert(deserializedTwice.attempt.unsafeRunSync.isLeft)
    }
  }

  test("Deserializer#bytes") {
    forAll { (topic: String, headers: Headers, bytes: Array[Byte]) =>
      val deserialized =
        Deserializer
          .bytes[Id]
          .deserialize(topic, headers, bytes)

      deserialized.get shouldBe bytes
    }
  }

  test("Deserializer#defer") {
    forAll { (topic: String, headers: Headers, bytes: Array[Byte]) =>
      var deserialized = false

      val deserializer =
        Deserializer
          .lift[Eval, Array[Byte]] { bytes =>
            deserialized = true
            Eval.now(bytes)
          }
          .defer

      val eval = deserializer.deserialize(topic, headers, bytes)
      assert(!deserialized)
      eval.value
      assert(deserialized)
    }
  }

  test("Deserializer#fail") {
    val deserializer =
      Deserializer.fail[IO, Int](new RuntimeException)

    forAll { (topic: String, headers: Headers, bytes: Array[Byte]) =>
      val deserialized = deserializer.deserialize(topic, headers, bytes)
      assert(deserialized.attempt.unsafeRunSync.isLeft)
    }
  }

  test("Deserializer#headers") {
    val deserializer =
      Deserializer.headers { headers =>
        headers("format").map(_.as[String]) match {
          case Some("int") => Deserializer[IO, Int]
          case _           => Deserializer[IO, String].map(_.toInt).attempt
        }
      }

    forAll { (topic: String, i: Int) =>
      val headers = Header("format", "int").headers
      val serialized = Serializer[Id, Int].serialize(topic, Headers.empty, i)
      val deserialized = deserializer.deserialize(topic, headers, serialized)
      deserialized.attempt.unsafeRunSync shouldBe Right(i)
    }

    forAll { (topic: String, i: Int) =>
      val serialized =
        Serializer[Id, String].contramap[Int](_.toString).serialize(topic, Headers.empty, i)
      val deserialized =
        Deserializer[IO, String].map(_.toInt).attempt.deserialize(topic, Headers.empty, serialized)
      deserialized.attempt.unsafeRunSync shouldBe Right(i)
    }
  }

  test("Deserializer#identity") {
    forAll { (topic: String, headers: Headers, bytes: Array[Byte]) =>
      val deserialized =
        Deserializer
          .identity[Id]
          .deserialize(topic, headers, bytes)

      deserialized shouldBe bytes
    }
  }

  test("Deserializer#suspend") {
    forAll { (topic: String, headers: Headers, bytes: Array[Byte]) =>
      var deserialized = false

      val deserializer =
        Deserializer
          .lift[IO, Array[Byte]] { bytes =>
            deserialized = true
            IO.pure(bytes)
          }
          .suspend

      val io = deserializer.deserialize(topic, headers, bytes)
      assert(!deserialized)
      io.unsafeRunSync
      assert(deserialized)
    }
  }

  test("Deserializer#topic") {
    val deserializer =
      Deserializer.topic {
        case "topic" => Deserializer[IO, Int]
        case _       => Deserializer[IO, String].map(_.toInt).attempt
      }

    forAll { i: Int =>
      val serialized = Serializer[Id, Int].serialize("topic", Headers.empty, i)
      val deserialized = deserializer.deserialize("topic", Headers.empty, serialized)
      deserialized.attempt.unsafeRunSync shouldBe Right(i)
    }

    forAll { (topic: String, i: Int) =>
      whenever(topic != "topic") {
        val serialized =
          Serializer[Id, String].contramap[Int](_.toString).serialize(topic, Headers.empty, i)
        val deserialized =
          Deserializer[IO, String]
            .map(_.toInt)
            .attempt
            .deserialize(topic, Headers.empty, serialized)

        deserialized.attempt.unsafeRunSync shouldBe Right(i)
      }
    }
  }

  test("Deserializer#option") {
    val deserializer =
      Deserializer[Id, Option[String]]

    deserializer.deserialize("topic", Headers.empty, null) shouldBe None

    forAll { s: String =>
      val serialized = Serializer[Id, String].serialize("topic", Headers.empty, s)
      deserializer.deserialize("topic", Headers.empty, serialized) shouldBe Some(s)
    }
  }

  test("Deserializer#unit") {
    forAll { bytes: Array[Byte] =>
      Deserializer[Id, Unit].deserialize("topic", Headers.empty, bytes) shouldBe (())
    }
  }

  test("Deserializer#toString") {
    assert(Deserializer[Id, String].toString startsWith "Deserializer$")
  }
}

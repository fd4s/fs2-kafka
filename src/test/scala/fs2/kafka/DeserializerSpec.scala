package fs2.kafka

import cats.laws.discipline._
import scala.collection.JavaConverters._

final class DeserializerSpec extends BaseCatsSpec {
  checkAll("Deserializer", MonadTests[Deserializer].monad[String, String, String])

  // Always only returns `Unit`. Invoked to include in coverage.
  test("Deserializer#configure") { Deserializer[String].configure(Map.empty.asJava, true) }
  test("Deserializer#close") { Deserializer[String].close() }

  test("Deserializer#delay") {
    var deserialized = false

    val deserializer =
      Deserializer.lift { bytes =>
        deserialized = true
        bytes
      }.delay

    val eval = deserializer.deserialize("", Array())
    assert(!deserialized)

    val result = eval.value
    assert(deserialized)
    assert(result.isEmpty)
  }

  test("Deserializer#headers") {
    val deserializer =
      Deserializer.headers { headers =>
        headers("format").map(_.as[String]) match {
          case Some("int") => Deserializer.attempt[Int]
          case _           => Deserializer[String].map(_.toInt).attempt
        }
      }

    forAll { (topic: String, i: Int) =>
      val headers = Header("format", "int").headers
      val serialized = Serializer[Int].serialize(topic, i)
      val deserialized = deserializer.deserialize(topic, headers, serialized)
      deserialized shouldBe Right(i)
    }

    forAll { (topic: String, i: Int) =>
      val serialized = Serializer[String].contramap[Int](_.toString).serialize(topic, i)
      val deserialized = Deserializer[String].map(_.toInt).attempt.deserialize(topic, serialized)
      deserialized shouldBe Right(i)
    }
  }

  test("Deserializer#topic") {
    val deserializer =
      Deserializer.topic {
        case "topic" => Deserializer.attempt[Int]
        case _       => Deserializer[String].map(_.toInt).attempt
      }

    forAll { i: Int =>
      val serialized = Serializer[Int].serialize("topic", i)
      val deserialized = deserializer.deserialize("topic", serialized)
      deserialized shouldBe Right(i)
    }

    forAll { (topic: String, i: Int) =>
      whenever(topic != "topic") {
        val serialized = Serializer[String].contramap[Int](_.toString).serialize(topic, i)
        val deserialized = Deserializer[String].map(_.toInt).attempt.deserialize(topic, serialized)
        deserialized shouldBe Right(i)
      }
    }
  }

  test("Deserializer#option") {
    val deserializer =
      Deserializer[Option[String]]

    deserializer.deserialize("topic", null) shouldBe None

    forAll { s: String =>
      deserializer.deserialize("topic", s.getBytes) shouldBe Some(s)
    }
  }

  test("Deserializer#unit") {
    forAll { bytes: Array[Byte] =>
      Deserializer[Unit].deserialize("topic", bytes) shouldBe (())
    }
  }

  test("Deserializer#toString") {
    assert(Deserializer[String].toString startsWith "Deserializer$")
  }
}

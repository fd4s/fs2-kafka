package fs2.kafka

import cats._
import cats.effect.IO
import cats.tests._
import org.scalacheck._
import scala.util.Try

trait BaseCatsSpec extends CatsSuite with BaseGenerators {
  implicit def deserializerEq[A](implicit A: Eq[IO[A]]): Eq[Deserializer[IO, A]] =
    Eq.instance { (d1, d2) =>
      Try {
        forAll { (topic: String, headers: Headers, bytes: Array[Byte]) =>
          val r1 = d1.deserialize(topic, headers, bytes)
          val r2 = d2.deserialize(topic, headers, bytes)
          (r1 === r2) shouldBe true
        }
      }.isSuccess
    }

  implicit val deserializerUnitArbitrary: Arbitrary[Deserializer[IO, Unit]] =
    Arbitrary(Gen.const(Deserializer.unit[IO]))

  implicit def serializerEq[A](implicit A: Arbitrary[A]): Eq[Serializer[Id, A]] =
    Eq.instance { (s1, s2) =>
      Try {
        forAll { (topic: String, headers: Headers, a: A) =>
          val r1 = s1.serialize(topic, headers, a)
          val r2 = s2.serialize(topic, headers, a)
          r1 should contain theSameElementsInOrderAs (r2)
        }
      }.isSuccess
    }

  implicit def headerSerializerEq[A](implicit A: Arbitrary[A]): Eq[HeaderSerializer[A]] =
    Eq.instance { (s1, s2) =>
      Try {
        forAll { a: A =>
          val r1 = s1.serialize(a)
          val r2 = s2.serialize(a)
          r1 should contain theSameElementsInOrderAs (r2)
        }
      }.isSuccess
    }

  implicit def headerDeserializerEq[A](implicit A: Eq[A]): Eq[HeaderDeserializer[A]] =
    Eq.instance { (d1, d2) =>
      Try {
        forAll { bytes: Array[Byte] =>
          val r1 = d1.deserialize(bytes)
          val r2 = d2.deserialize(bytes)
          (r1 === r2) shouldBe true
        }
      }.isSuccess
    }
}

package fs2.kafka

import cats.effect.IO

class RecordSerializerSpec extends BaseSpec {

  import cats.effect.unsafe.implicits.global

  describe("RecordSerializer#transform") {
    it("should transform the RecordSerializer applying the function to inner Serializers") {

      val intRecordSer: RecordSerializer[IO, Int] =
        RecordSerializer
          .lift(Serializer[IO, String])
          .transform(_.contramap(_.toString))

      intRecordSer.forKey
        .use(_.serialize("T1", Headers.empty, 1))
        .unsafeRunSync() shouldBe "1".getBytes
    }
  }

  describe("RecordSerializer#option") {
    it("should transform the RecordSerializer[F, T] to RecordSerializer[F, Option[T]]") {

      val optStrRecordSer: RecordSerializer[IO, Option[String]] =
        RecordSerializer
          .lift(Serializer[IO, String])
          .option

      optStrRecordSer.forKey
        .use(_.serialize("T1", Headers.empty, Some("1")))
        .unsafeRunSync() shouldBe "1".getBytes

      optStrRecordSer.forKey
        .use(_.serialize("T1", Headers.empty, None))
        .unsafeRunSync() shouldBe null
    }
  }
}

package fs2.kafka

import cats.Applicative
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.{Arbitrary, Gen}
import java.nio.charset._

trait BaseGenerators {
  val genTopicPartition: Gen[TopicPartition] =
    for {
      topic <- arbitrary[String]
      partition <- Gen.chooseNum(0, Int.MaxValue)
    } yield new TopicPartition(topic, partition)

  implicit val arbTopicPartition: Arbitrary[TopicPartition] =
    Arbitrary(genTopicPartition)

  val genOffsetAndMetadata: Gen[OffsetAndMetadata] =
    for {
      offset <- Gen.chooseNum(1L, Long.MaxValue)
      metadata <- arbitrary[String]
    } yield new OffsetAndMetadata(offset, metadata)

  implicit val arbOffsetAndMetadata: Arbitrary[OffsetAndMetadata] =
    Arbitrary(genOffsetAndMetadata)

  def genCommittableOffset[F[_]](
    implicit F: Applicative[F]
  ): Gen[CommittableOffset[F]] =
    for {
      topicPartition <- genTopicPartition
      offsetAndMetadata <- genOffsetAndMetadata
    } yield
      CommittableOffset[F](
        topicPartition = topicPartition,
        offsetAndMetadata = offsetAndMetadata,
        commit = _ => F.unit
      )

  implicit def arbCommittableOffset[F[_]](
    implicit F: Applicative[F]
  ): Arbitrary[CommittableOffset[F]] =
    Arbitrary(genCommittableOffset[F])

  def genCommittableOffsetBatch[F[_]](
    implicit F: Applicative[F]
  ): Gen[CommittableOffsetBatch[F]] =
    arbitrary[Map[TopicPartition, OffsetAndMetadata]]
      .map(CommittableOffsetBatch[F](_, _ => F.unit))

  implicit def arbCommittableOffsetBatch[F[_]](
    implicit F: Applicative[F]
  ): Arbitrary[CommittableOffsetBatch[F]] =
    Arbitrary(genCommittableOffsetBatch[F])

  val genCharset: Gen[Charset] =
    Gen.oneOf(
      StandardCharsets.US_ASCII,
      StandardCharsets.ISO_8859_1,
      StandardCharsets.UTF_8,
      StandardCharsets.UTF_16BE,
      StandardCharsets.UTF_16LE,
      StandardCharsets.UTF_16
    )

  implicit val arbCharset: Arbitrary[Charset] =
    Arbitrary(genCharset)

  val genDeserializerString: Gen[Deserializer[String]] =
    genCharset.map(Deserializer.string)

  implicit val arbDeserializerString: Arbitrary[Deserializer[String]] =
    Arbitrary(genDeserializerString)

  implicit def arbDeserializerCombine[A, B](
    implicit arbB: Arbitrary[Deserializer[B]],
    arbABA: Arbitrary[(A, B) => A]
  ): Arbitrary[Deserializer[A => A]] =
    Arbitrary {
      for {
        deserializer <- arbitrary[Deserializer[B]]
        combine <- arbitrary[(A, B) => A]
      } yield {
        Deserializer.instance { (topic, headers, bytes) => (a: A) =>
          combine(a, deserializer.deserialize(topic, headers, bytes))
        }
      }
    }

  val genSerializerString: Gen[Serializer[String]] =
    genCharset.map(Serializer.string)

  implicit val arbSerializerString: Arbitrary[Serializer[String]] =
    Arbitrary(genSerializerString)

  val genHeader: Gen[Header] =
    for {
      key <- arbitrary[String]
      value <- arbitrary[Array[Byte]]
    } yield Header(key, value)

  implicit val arbHeader: Arbitrary[Header] =
    Arbitrary(genHeader)

  val genHeaders: Gen[Headers] =
    Gen.listOf(genHeader).map(Headers.fromSeq)

  implicit val arbHeaders: Arbitrary[Headers] =
    Arbitrary(genHeaders)

  val genHeaderSerializerString: Gen[HeaderSerializer[String]] =
    genCharset.map(HeaderSerializer.string)

  implicit val arbHeaderSerializerString: Arbitrary[HeaderSerializer[String]] =
    Arbitrary(genHeaderSerializerString)

  val genHeaderDeserializerString: Gen[HeaderDeserializer[String]] =
    genCharset.map(HeaderDeserializer.string)

  implicit val arbHeaderDeserializerString: Arbitrary[HeaderDeserializer[String]] =
    Arbitrary(genHeaderDeserializerString)

  implicit def arbHeaderDeserializerCombine[A, B](
    implicit arbB: Arbitrary[HeaderDeserializer[B]],
    arbABA: Arbitrary[(A, B) => A]
  ): Arbitrary[HeaderDeserializer[A => A]] =
    Arbitrary {
      for {
        deserializer <- arbitrary[HeaderDeserializer[B]]
        combine <- arbitrary[(A, B) => A]
      } yield {
        HeaderDeserializer.instance { bytes => (a: A) =>
          combine(a, deserializer.deserialize(bytes))
        }
      }
    }
}

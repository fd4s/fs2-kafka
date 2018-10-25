package fs2.kafka

import cats.Applicative
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.{Arbitrary, Gen}

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
}

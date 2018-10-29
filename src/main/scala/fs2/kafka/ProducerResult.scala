package fs2.kafka

import cats.Show
import cats.instances.string._
import cats.syntax.show._
import fs2.kafka.internal.instances._
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}

sealed abstract class ProducerResult[K, V, P] {
  def passthrough: P
}

object ProducerResult {
  sealed abstract case class Single[K, V, P](
    metadata: RecordMetadata,
    record: ProducerRecord[K, V],
    override val passthrough: P
  ) extends ProducerResult[K, V, P] {
    override def toString: String =
      s"Single($metadata -> $record, $passthrough)"
  }

  sealed abstract case class MultiplePart[K, V](
    metadata: RecordMetadata,
    record: ProducerRecord[K, V]
  ) {
    override def toString: String =
      s"$metadata -> $record"
  }

  object MultiplePart {
    implicit def multiplePartShow[K, V](
      implicit
      K: Show[K],
      V: Show[V]
    ): Show[MultiplePart[K, V]] = Show.show { mp =>
      show"${mp.metadata} -> ${mp.record}"
    }
  }

  sealed abstract case class Multiple[K, V, P](
    parts: List[MultiplePart[K, V]],
    override val passthrough: P
  ) extends ProducerResult[K, V, P] {
    override def toString: String =
      s"Multiple(${parts.mkString(", ")}, $passthrough)"
  }

  sealed abstract case class Passthrough[K, V, P](
    override val passthrough: P
  ) extends ProducerResult[K, V, P] {
    override def toString: String =
      s"Passthrough($passthrough)"
  }

  def single[K, V, P](
    metadata: RecordMetadata,
    record: ProducerRecord[K, V],
    passthrough: P
  ): ProducerResult[K, V, P] =
    new Single(metadata, record, passthrough) {}

  def multiple[K, V, P](
    parts: List[MultiplePart[K, V]],
    passthrough: P
  ): ProducerResult[K, V, P] =
    new Multiple(parts, passthrough) {}

  def multiplePart[K, V](
    metadata: RecordMetadata,
    record: ProducerRecord[K, V]
  ): MultiplePart[K, V] =
    new MultiplePart(metadata, record) {}

  def passthrough[K, V, P](
    passthrough: P
  ): ProducerResult[K, V, P] =
    new Passthrough[K, V, P](passthrough) {}

  implicit def producerResultShow[K, V, P](
    implicit
    K: Show[K],
    V: Show[V],
    P: Show[P]
  ): Show[ProducerResult[K, V, P]] = Show.show {
    case Single(metadata, record, passthrough) =>
      show"Single($metadata -> $record, $passthrough)"
    case Multiple(parts, passthrough) =>
      show"Multiple(${parts.map(_.show).mkString(", ")}, $passthrough)"
    case Passthrough(passthrough) =>
      show"Passthrough($passthrough)"
  }
}

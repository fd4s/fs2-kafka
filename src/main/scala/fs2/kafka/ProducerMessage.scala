package fs2.kafka

import cats.Show
import cats.instances.string._
import cats.syntax.show._
import fs2.kafka.internal.instances._
import org.apache.kafka.clients.producer.ProducerRecord

sealed abstract class ProducerMessage[K, V, P] {
  def passthrough: P
}

object ProducerMessage {
  sealed abstract case class Single[K, V, P](
    record: ProducerRecord[K, V],
    override val passthrough: P
  ) extends ProducerMessage[K, V, P] {
    override def toString: String =
      s"Single($record, $passthrough)"
  }

  sealed abstract case class Multiple[K, V, P](
    records: List[ProducerRecord[K, V]],
    override val passthrough: P
  ) extends ProducerMessage[K, V, P] {
    override def toString: String =
      s"Multiple(${records.mkString(", ")}, $passthrough)"
  }

  sealed abstract case class Passthrough[K, V, P](
    override val passthrough: P
  ) extends ProducerMessage[K, V, P] {
    override def toString: String =
      s"Passthrough($passthrough)"
  }

  def single[K, V, P](
    record: ProducerRecord[K, V],
    passthrough: P
  ): ProducerMessage[K, V, P] =
    new Single(record, passthrough) {}

  def multiple[K, V, P](
    records: List[ProducerRecord[K, V]],
    passthrough: P
  ): ProducerMessage[K, V, P] =
    new Multiple(records, passthrough) {}

  def passthrough[K, V, P](
    passthrough: P
  ): ProducerMessage[K, V, P] =
    new Passthrough[K, V, P](passthrough) {}

  implicit def producerMessageShow[K, V, P](
    implicit
    K: Show[K],
    V: Show[V],
    P: Show[P]
  ): Show[ProducerMessage[K, V, P]] = Show.show {
    case Single(record, passthrough) =>
      show"Single($record, $passthrough)"
    case Multiple(records, passthrough) =>
      show"Multiple(${records.map(_.show).mkString(", ")}, $passthrough)"
    case Passthrough(passthrough) =>
      show"Passthrough($passthrough)"
  }
}

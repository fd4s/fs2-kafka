package fs2.kafka

import cats.Show
import cats.syntax.show._
import fs2.kafka.internal.instances._
import org.apache.kafka.clients.producer.ProducerRecord

sealed abstract class ProducerMessage[K, V, P] {
  def passthrough: P
}

object ProducerMessage {
  sealed abstract class Single[K, V, P](
    val record: ProducerRecord[K, V],
    override val passthrough: P
  ) extends ProducerMessage[K, V, P] {
    override def toString: String =
      s"Single($record, $passthrough)"
  }

  object Single {
    def unapply[K, V, P](
      message: ProducerMessage[K, V, P]
    ): Option[(ProducerRecord[K, V], P)] = message match {
      case single: Single[K, V, P] => Some((single.record, single.passthrough))
      case _                       => None
    }
  }

  sealed abstract class Multiple[K, V, P](
    val records: List[ProducerRecord[K, V]],
    override val passthrough: P
  ) extends ProducerMessage[K, V, P] {
    override def toString: String =
      records.mkString("Multiple(", ", ", s", $passthrough)")
  }

  object Multiple {
    def unapply[K, V, P](
      message: ProducerMessage[K, V, P]
    ): Option[(List[ProducerRecord[K, V]], P)] = message match {
      case multiple: Multiple[K, V, P] => Some((multiple.records, multiple.passthrough))
      case _                           => None
    }
  }

  sealed abstract class Passthrough[K, V, P](
    override val passthrough: P
  ) extends ProducerMessage[K, V, P] {
    override def toString: String =
      s"Passthrough($passthrough)"
  }

  object Passthrough {
    def unapply[K, V, P](
      message: ProducerMessage[K, V, P]
    ): Option[P] = message match {
      case passthrough: Passthrough[K, V, P] => Some(passthrough.passthrough)
      case _                                 => None
    }
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
      records.map(_.show).mkString("Multiple(", ", ", s", $passthrough)")
    case Passthrough(passthrough) =>
      show"Passthrough($passthrough)"
  }
}

package fs2.kafka

import org.apache.kafka.clients.producer.ProducerRecord

sealed abstract class ProducerMessage[K, V, P] {
  def passthrough: P
}

object ProducerMessage {
  private[kafka] final case class Single[K, V, P](
    record: ProducerRecord[K, V],
    override val passthrough: P
  ) extends ProducerMessage[K, V, P]

  private[kafka] final case class Multiple[K, V, P](
    records: List[ProducerRecord[K, V]],
    override val passthrough: P
  ) extends ProducerMessage[K, V, P]

  private[kafka] final case class Passthrough[K, V, P](
    override val passthrough: P
  ) extends ProducerMessage[K, V, P]

  def single[K, V, P](
    record: ProducerRecord[K, V],
    passthrough: P
  ): ProducerMessage[K, V, P] =
    Single(record, passthrough)

  def multiple[K, V, P](
    records: List[ProducerRecord[K, V]],
    passthrough: P
  ): ProducerMessage[K, V, P] =
    Multiple(records, passthrough)

  def passthrough[K, V, P](
    passthrough: P
  ): ProducerMessage[K, V, P] =
    Passthrough(passthrough)
}

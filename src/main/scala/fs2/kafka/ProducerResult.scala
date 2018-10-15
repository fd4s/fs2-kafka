package fs2.kafka

import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}

sealed abstract class ProducerResult[K, V, P] {
  def passthrough: P
}

object ProducerResult {
  sealed abstract case class Single[K, V, P](
    metadata: RecordMetadata,
    record: ProducerRecord[K, V],
    override val passthrough: P
  ) extends ProducerResult[K, V, P]

  sealed abstract case class MultiplePart[K, V](
    metadata: RecordMetadata,
    record: ProducerRecord[K, V]
  )

  sealed abstract case class Multiple[K, V, P](
    parts: List[MultiplePart[K, V]],
    override val passthrough: P
  ) extends ProducerResult[K, V, P]

  sealed abstract case class Passthrough[K, V, P](
    override val passthrough: P
  ) extends ProducerResult[K, V, P]

  private[kafka] def single[K, V, P](
    metadata: RecordMetadata,
    record: ProducerRecord[K, V],
    passthrough: P
  ): ProducerResult[K, V, P] =
    new Single(metadata, record, passthrough) {}

  private[kafka] def multiple[K, V, P](
    parts: List[MultiplePart[K, V]],
    passthrough: P
  ): ProducerResult[K, V, P] =
    new Multiple(parts, passthrough) {}

  private[kafka] def multiplePart[K, V, P](
    metadata: RecordMetadata,
    record: ProducerRecord[K, V]
  ): MultiplePart[K, V] =
    new MultiplePart(metadata, record) {}

  private[kafka] def passthrough[K, V, P](
    passthrough: P
  ): ProducerResult[K, V, P] =
    new Passthrough[K, V, P](passthrough) {}
}

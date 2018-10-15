package fs2.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord

sealed abstract class CommittableMessage[F[_], K, V] {
  def record: ConsumerRecord[K, V]

  def committableOffset: CommittableOffset[F]
}

object CommittableMessage {
  private[this] final class CommittableMessageImpl[F[_], K, V](
    override val record: ConsumerRecord[K, V],
    override val committableOffset: CommittableOffset[F]
  ) extends CommittableMessage[F, K, V]

  private[kafka] def apply[F[_], K, V](
    record: ConsumerRecord[K, V],
    committableOffset: CommittableOffset[F]
  ): CommittableMessage[F, K, V] =
    new CommittableMessageImpl(
      record = record,
      committableOffset = committableOffset
    )
}

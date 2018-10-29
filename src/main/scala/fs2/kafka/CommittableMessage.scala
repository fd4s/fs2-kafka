package fs2.kafka

import cats.Show
import cats.syntax.show._
import fs2.kafka.internal.instances._
import org.apache.kafka.clients.consumer.ConsumerRecord

sealed abstract class CommittableMessage[F[_], K, V] {
  def record: ConsumerRecord[K, V]

  def committableOffset: CommittableOffset[F]
}

object CommittableMessage {
  private[this] final class CommittableMessageImpl[F[_], K, V](
    override val record: ConsumerRecord[K, V],
    override val committableOffset: CommittableOffset[F]
  ) extends CommittableMessage[F, K, V] {
    override def toString: String =
      s"CommittableMessage($record, $committableOffset)"
  }

  def apply[F[_], K, V](
    record: ConsumerRecord[K, V],
    committableOffset: CommittableOffset[F]
  ): CommittableMessage[F, K, V] =
    new CommittableMessageImpl(
      record = record,
      committableOffset = committableOffset
    )

  implicit def committableMessageShow[F[_], K, V](
    implicit
    K: Show[K],
    V: Show[V]
  ): Show[CommittableMessage[F, K, V]] = Show.show { cm =>
    show"CommittableMessage(${cm.record}, ${cm.committableOffset})"
  }
}

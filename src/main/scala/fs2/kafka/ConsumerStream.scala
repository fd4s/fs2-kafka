package fs2.kafka

import cats.effect.{ConcurrentEffect, ContextShift, Timer}
import fs2.Stream

final class ConsumerStream[F[_]] private[kafka] {
  def using[K, V](settings: ConsumerSettings[K, V])(
    implicit F: ConcurrentEffect[F],
    context: ContextShift[F],
    timer: Timer[F]
  ): Stream[F, KafkaConsumer[F, K, V]] =
    consumerStream(settings)
}

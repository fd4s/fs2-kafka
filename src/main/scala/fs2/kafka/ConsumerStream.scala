package fs2.kafka

import cats.effect.{ConcurrentEffect, Timer}
import fs2.Stream

final class ConsumerStream[F[_]] private[kafka] {
  def using[K, V](settings: ConsumerSettings[K, V])(
    implicit F: ConcurrentEffect[F],
    timer: Timer[F]
  ): Stream[F, KafkaConsumer[F, K, V]] =
    consumerStream(settings)
}

package fs2.kafka

import cats.effect.{ConcurrentEffect, ContextShift, Resource, Timer}

final class ConsumerResource[F[_]] private[kafka] {
  def using[K, V](settings: ConsumerSettings[K, V])(
    implicit F: ConcurrentEffect[F],
    context: ContextShift[F],
    timer: Timer[F]
  ): Resource[F, KafkaConsumer[F, K, V]] =
    consumerResource(settings)

  override def toString: String =
    "ConsumerResource$" + System.identityHashCode(this)
}

package fs2.kafka

import cats.effect.{ConcurrentEffect, Resource}

final class ProducerResource[F[_]] private[kafka] {
  def using[K, V](settings: ProducerSettings[K, V])(
    implicit F: ConcurrentEffect[F]
  ): Resource[F, KafkaProducer[F, K, V]] =
    producerResource(settings)

  override def toString: String =
    "ProducerResource$" + System.identityHashCode(this)
}

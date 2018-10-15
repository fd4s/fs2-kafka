package fs2.kafka

import cats.effect.ConcurrentEffect
import fs2.Stream

final class ProducerStream[F[_]] private[kafka] {
  def using[K, V](settings: ProducerSettings[K, V])(
    implicit F: ConcurrentEffect[F]
  ): Stream[F, KafkaProducer[F, K, V]] =
    producerStream(settings)
}

package fs2

import cats.effect.{ConcurrentEffect, Timer}

package object kafka {
  def consumerStream[F[_], K, V](settings: ConsumerSettings[K, V])(
    implicit F: ConcurrentEffect[F],
    timer: Timer[F]
  ): Stream[F, KafkaConsumer[F, K, V]] =
    KafkaConsumer.consumerStream(settings)

  def consumerStream[F[_]]: ConsumerStream[F] =
    new ConsumerStream[F]

  def producerStream[F[_], K, V](settings: ProducerSettings[K, V])(
    implicit F: ConcurrentEffect[F]
  ): Stream[F, KafkaProducer[F, K, V]] =
    KafkaProducer.producerStream(settings)

  def producerStream[F[_]]: ProducerStream[F] =
    new ProducerStream[F]
}

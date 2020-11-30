/*
 * Copyright 2018-2020 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect._
import cats.implicits._
import fs2.Stream
import fs2.kafka.internal._

sealed abstract class KafkaProducerConnection[F[_]] {
  def withSerializers[K, V](
    keySerializer: Serializer[F, K],
    valueSerializer: Serializer[F, V]
  ): KafkaProducer.Metrics[F, K, V]

  def withSerializersFrom[K, V](
    settings: ProducerSettings[F, K, V]
  ): F[KafkaProducer.Metrics[F, K, V]]
}

object KafkaProducerConnection {
  def stream[F[_]](
    settings: ProducerSettings[F, _, _]
  )(
    implicit F: Concurrent[F],
    context: ContextShift[F]
  ): Stream[F, KafkaProducerConnection[F]] = Stream.resource(resource(settings))

  def resource[F[_]](
    settings: ProducerSettings[F, _, _]
  )(
    implicit F: Concurrent[F],
    context: ContextShift[F]
  ): Resource[F, KafkaProducerConnection[F]] =
    WithProducer(settings).map { withProducer =>
      new KafkaProducerConnection[F] {
        override def withSerializers[K, V](
          keySerializer: Serializer[F, K],
          valueSerializer: Serializer[F, V]
        ): KafkaProducer.Metrics[F, K, V] =
          KafkaProducer.from(withProducer, keySerializer, valueSerializer)

        override def withSerializersFrom[K, V](
          settings: ProducerSettings[F, K, V]
        ): F[KafkaProducer.Metrics[F, K, V]] =
          (settings.keySerializer, settings.valueSerializer).mapN(withSerializers)
      }
    }
}

/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect._
import cats.implicits._
import fs2.Stream
import fs2.kafka.internal._

/**
  * [[KafkaProducerConnection]] represents a connection to a Kafka broker
  * that can be used to create [[KafkaProducer]] instances. All [[KafkaProducer]]
  * instances created from an given [[KafkaProducerConnection]] share a single
  * underlying connection.
  */
sealed abstract class KafkaProducerConnection[F[_]] {

  /**
    * Creates a new [[KafkaProducer]]  using the provided serializers.
    *
    * {{{
    * KafkaProducerConnection.stream[F].using(settings).map(_.withSerializers(keySerializer, valueSerializer))
    * }}}
    */
  def withSerializers[K, V](
    keySerializer: Serializer[F, K],
    valueSerializer: Serializer[F, V]
  ): KafkaProducer.Metrics[F, K, V]

  /**
    * Creates a new [[KafkaProducer]] in the `F` context,
    * using serializers from the specified [[ProducerSettings]].
    *
    * {{{
    * KafkaProducerConnection.stream[F].using(settings).evalMap(_.withSerializersFrom(settings))
    * }}}
    */
  def withSerializersFrom[K, V](
    settings: ProducerSettings[F, K, V]
  ): F[KafkaProducer.Metrics[F, K, V]]
}

object KafkaProducerConnection {

  /**
    * Creates a new [[KafkaProducerConnection]] in the `Stream` context,
    * using the specified [[ProducerSettings]].
    *
    * {{{
    * KafkaProducerConnection.stream[F].using(settings)
    * }}}
    */
  def stream[F[_]](
    settings: ProducerSettings[F, _, _]
  )(
    implicit F: ConcurrentEffect[F],
    context: ContextShift[F]
  ): Stream[F, KafkaProducerConnection[F]] = Stream.resource(resource(settings))

  /**
    * Creates a new [[KafkaProducerConnection]] in the `Resource` context,
    * using the specified [[ProducerSettings]].
    *
    * {{{
    * KafkaProducerConnection.resource[F].using(settings)
    * }}}
    */
  def resource[F[_]](
    settings: ProducerSettings[F, _, _]
  )(
    implicit F: ConcurrentEffect[F],
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

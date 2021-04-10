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
import fs2.kafka.producer.MkProducer

import scala.annotation.nowarn

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
    * KafkaProducerConnection.stream[F](settings)
    * }}}
    */
  def stream[F[_]](
    settings: ProducerSettings[F, _, _]
  )(
    implicit F: Async[F],
    mk: MkProducer[F]
  ): Stream[F, KafkaProducerConnection[F]] = Stream.resource(resource(settings)(F, mk))

  /**
    * Creates a new [[KafkaProducerConnection]] in the `Resource` context,
    * using the specified [[ProducerSettings]].
    *
    * {{{
    * KafkaProducerConnection.resource[F](settings)
    * }}}
    */
  def resource[F[_]](
    settings: ProducerSettings[F, _, _]
  )(
    implicit F: Async[F],
    mk: MkProducer[F]
  ): Resource[F, KafkaProducerConnection[F]] =
    WithProducer(mk, settings).map { withProducer =>
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

  /*
   * Prevents the default `MkProducer` instance from being implicitly available
   * to code defined in this object, ensuring factory methods require an instance
   * to be provided at the call site.
   */
  @nowarn("cat=unused")
  implicit private def mkAmbig1[F[_]]: MkProducer[F] =
    throw new AssertionError("should not be used")
  @nowarn("cat=unused")
  implicit private def mkAmbig2[F[_]]: MkProducer[F] =
    throw new AssertionError("should not be used")
}

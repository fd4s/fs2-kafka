/*
 * Copyright 2018-2022 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect._
import cats.syntax.all._
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
    keySerializer: KeySerializer[F, K],
    valueSerializer: ValueSerializer[F, V]
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
  ): Stream[F, KafkaProducerConnection[F]] = streamIn(settings)(F, F, mk)

  /**
    * Like [[stream]], but allows use of different effect types for
    * the allocating `Stream` and the allocated `KafkaProducerConnection`.
    */
  def streamIn[F[_], G[_]](
    settings: ProducerSettings[G, _, _]
  )(
    implicit F: Async[F],
    G: Async[G],
    mk: MkProducer[F]
  ): Stream[F, KafkaProducerConnection[G]] = Stream.resource(resourceIn(settings)(F, G, mk))

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
    resourceIn(settings)(F, F, mk)

  /**
    * Like [[resource]], but allows use of different effect types for
    * the allocating `Resource` and the allocated `KafkaProducerConnection`.
    */
  def resourceIn[F[_], G[_]](
    settings: ProducerSettings[G, _, _]
  )(
    implicit F: Async[F],
    G: Async[G],
    mk: MkProducer[F]
  ): Resource[F, KafkaProducerConnection[G]] =
    WithProducer(mk, settings).map { withProducer =>
      new KafkaProducerConnection[G] {
        override def withSerializers[K, V](
          keySerializer: KeySerializer[G, K],
          valueSerializer: ValueSerializer[G, V]
        ): KafkaProducer.Metrics[G, K, V] =
          KafkaProducer.from(withProducer, keySerializer, valueSerializer)

        override def withSerializersFrom[K, V](
          settings: ProducerSettings[G, K, V]
        ): G[KafkaProducer.Metrics[G, K, V]] =
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

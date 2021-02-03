/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2

import cats.effect._
import scala.concurrent.duration.FiniteDuration

package object kafka {
  type Id[+A] = A

  /** Alias for Java Kafka `Consumer[Array[Byte], Array[Byte]]`. */
  type KafkaByteConsumer =
    org.apache.kafka.clients.consumer.Consumer[Array[Byte], Array[Byte]]

  /** Alias for Java Kafka `Producer[Array[Byte], Array[Byte]]`. */
  type KafkaByteProducer =
    org.apache.kafka.clients.producer.Producer[Array[Byte], Array[Byte]]

  /** Alias for Java Kafka `Deserializer[A]`. */
  type KafkaDeserializer[A] =
    org.apache.kafka.common.serialization.Deserializer[A]

  /** Alias for Java Kafka `Serializer[A]`. */
  type KafkaSerializer[A] =
    org.apache.kafka.common.serialization.Serializer[A]

  /** Alias for Java Kafka `Header`. */
  type KafkaHeader =
    org.apache.kafka.common.header.Header

  /** Alias for Java Kafka `Headers`. */
  type KafkaHeaders =
    org.apache.kafka.common.header.Headers

  /** Alias for Java Kafka `ConsumerRecords[Array[Byte], Array[Byte]]`. */
  type KafkaByteConsumerRecords =
    org.apache.kafka.clients.consumer.ConsumerRecords[Array[Byte], Array[Byte]]

  /** Alias for Java Kafka `ConsumerRecord[Array[Byte], Array[Byte]]`. */
  type KafkaByteConsumerRecord =
    org.apache.kafka.clients.consumer.ConsumerRecord[Array[Byte], Array[Byte]]

  /** Alias for Java Kafka `ProducerRecord[Array[Byte], Array[Byte]]`. */
  type KafkaByteProducerRecord =
    org.apache.kafka.clients.producer.ProducerRecord[Array[Byte], Array[Byte]]

  /**
    * Commits offsets in batches of every `n` offsets or time window
    * of length `d`, whichever happens first. If there are no offsets
    * to commit within a time window, no attempt will be made to commit
    * offsets for that time window.
    */
  def commitBatchWithin[F[_]](n: Int, d: FiniteDuration)(
    implicit F: Concurrent[F],
    timer: Timer[F]
  ): Pipe[F, CommittableOffset[F], Unit] =
    _.groupWithin(n, d).evalMap(CommittableOffsetBatch.fromFoldable(_).commit)

  @deprecated("use KafkaProducer.pipe", "1.2.0")
  def produce[F[_]: ConcurrentEffect: ContextShift, K, V, P](
    settings: ProducerSettings[F, K, V]
  ): Pipe[F, ProducerRecords[K, V, P], ProducerResult[K, V, P]] =
    KafkaProducer.pipe(settings)

  @deprecated("use KafkaProducer.pipe", "1.2.0")
  def produce[F[_]: ConcurrentEffect, K, V, P](
    settings: ProducerSettings[F, K, V],
    producer: KafkaProducer[F, K, V]
  ): Pipe[F, ProducerRecords[K, V, P], ProducerResult[K, V, P]] =
    KafkaProducer.pipe(settings, producer)

  @deprecated("use KafkaAdminClient.resource", "1.2.0")
  def adminClientResource[F[_]](settings: AdminClientSettings[F])(
    implicit F: Concurrent[F],
    context: ContextShift[F]
  ): Resource[F, KafkaAdminClient[F]] =
    KafkaAdminClient.resource(settings)

  @deprecated("use KafkaAdminClient.stream", "1.2.0")
  def adminClientStream[F[_]](settings: AdminClientSettings[F])(
    implicit F: Concurrent[F],
    context: ContextShift[F]
  ): Stream[F, KafkaAdminClient[F]] =
    KafkaAdminClient.stream(settings)

  @deprecated("use KafkaConsumer.resource", "1.2.0")
  def consumerResource[F[_], K, V](settings: ConsumerSettings[F, K, V])(
    implicit F: ConcurrentEffect[F],
    context: ContextShift[F],
    timer: Timer[F]
  ): Resource[F, KafkaConsumer[F, K, V]] =
    KafkaConsumer.resource(settings)

  @deprecated("use KafkaConsumer.resource", "1.2.0")
  def consumerResource[F[_]](implicit F: ConcurrentEffect[F]): ConsumerResource[F] =
    KafkaConsumer.resource(F)

  @deprecated("use KafkaConsumer.stream", "1.2.0")
  def consumerStream[F[_], K, V](settings: ConsumerSettings[F, K, V])(
    implicit F: ConcurrentEffect[F],
    context: ContextShift[F],
    timer: Timer[F]
  ): Stream[F, KafkaConsumer[F, K, V]] =
    KafkaConsumer.stream(settings)

  @deprecated("use KafkaConsumer.stream", "1.2.0")
  def consumerStream[F[_]](implicit F: ConcurrentEffect[F]): ConsumerStream[F] =
    KafkaConsumer.stream(F)

  @deprecated("use KafkaProducer.resource", "1.2.0")
  def producerResource[F[_]: ConcurrentEffect: ContextShift, K, V](
    settings: ProducerSettings[F, K, V]
  ): Resource[F, KafkaProducer.Metrics[F, K, V]] =
    KafkaProducer.resource(settings)

  @deprecated("use KafkaProducer.resource", "1.2.0")
  def producerResource[F[_]: ConcurrentEffect]: ProducerResource[F] = KafkaProducer.resource

  @deprecated("use KafkaProducer.stream", "1.2.0")
  def producerStream[F[_]: ConcurrentEffect: ContextShift, K, V](
    settings: ProducerSettings[F, K, V]
  ): Stream[F, KafkaProducer.Metrics[F, K, V]] = KafkaProducer.stream(settings)

  @deprecated("use KafkaProducer.stream", "1.2.0")
  def producerStream[F[_]: ConcurrentEffect]: ProducerStream[F] =
    KafkaProducer.stream[F]

  @deprecated("use TransactionalKafkaProducer.resource", "1.2.0")
  def transactionalProducerResource[F[_], K, V](
    settings: TransactionalProducerSettings[F, K, V]
  )(
    implicit F: ConcurrentEffect[F],
    context: ContextShift[F]
  ): Resource[F, TransactionalKafkaProducer[F, K, V]] =
    TransactionalKafkaProducer.resource(settings)

  @deprecated("use TransactionalKafkaProducer.resource", "1.2.0")
  def transactionalProducerResource[F[_]](
    implicit F: ConcurrentEffect[F]
  ): TransactionalProducerResource[F] =
    new TransactionalProducerResource(F)

  @deprecated("use TransactionalKafkaProducer.stream", "1.2.0")
  def transactionalProducerStream[F[_], K, V](
    settings: TransactionalProducerSettings[F, K, V]
  )(
    implicit F: ConcurrentEffect[F],
    context: ContextShift[F]
  ): Stream[F, TransactionalKafkaProducer[F, K, V]] =
    Stream.resource(TransactionalKafkaProducer.resource(settings))

  @deprecated("use TransactionalKafkaProducer.stream", "1.2.0")
  def transactionalProducerStream[F[_]](
    implicit F: ConcurrentEffect[F]
  ): TransactionalProducerStream[F] =
    new TransactionalProducerStream(F)
}

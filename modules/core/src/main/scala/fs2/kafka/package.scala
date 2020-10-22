/*
 * Copyright 2018-2020 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2

import cats.effect._
import scala.concurrent.duration.FiniteDuration
import cats.effect.unsafe.UnsafeRun

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
    implicit F: Temporal[F]
  ): Pipe[F, CommittableOffset[F], Unit] =
    _.groupWithin(n, d).evalMap(CommittableOffsetBatch.fromFoldable(_).commit)

  /**
    * Creates a [[KafkaProducer]] using the provided settings and
    * produces record in batches, limiting the number of records
    * in the same batch using [[ProducerSettings#parallelism]].
    */
  def produce[F[_], K, V, P](
    settings: ProducerSettings[F, K, V]
  )(
    implicit F: Async[F]
  ): Pipe[F, ProducerRecords[K, V, P], ProducerResult[K, V, P]] =
    records => producerStream(settings).flatMap(produce(settings, _).apply(records))

  /**
    * Produces records in batches using the provided [[KafkaProducer]].
    * The number of records in the same batch is limited using the
    * [[ProducerSettings#parallelism]] setting.
    */
  def produce[F[_], K, V, P](
    settings: ProducerSettings[F, K, V],
    producer: KafkaProducer[F, K, V]
  )(
    implicit F: Concurrent[F]
  ): Pipe[F, ProducerRecords[K, V, P], ProducerResult[K, V, P]] =
    _.evalMap(producer.produce).mapAsync(settings.parallelism)(identity)

  /**
    * Creates a new [[KafkaAdminClient]] in the `Resource` context,
    * using the specified [[AdminClientSettings]]. If working in a
    * `Stream` context, you might prefer [[adminClientStream]].
    */
  def adminClientResource[F[_]](settings: AdminClientSettings[F])(
    implicit F: Async[F]
  ): Resource[F, KafkaAdminClient[F]] =
    KafkaAdminClient.resource(settings)

  /**
    * Creates a new [[KafkaAdminClient]] in the `Stream` context,
    * using the specified [[AdminClientSettings]]. If you're not
    * working in a `Stream` context, you might instead prefer to
    * use the [[adminClientResource]] function.
    */
  def adminClientStream[F[_]](settings: AdminClientSettings[F])(
    implicit F: Async[F]
  ): Stream[F, KafkaAdminClient[F]] =
    Stream.resource(adminClientResource(settings))

  /**
    * Creates a new [[KafkaConsumer]] in the `Resource` context,
    * using the specified [[ConsumerSettings]]. Note that there
    * is another version where `F[_]` is specified explicitly and
    * the key and value type can be inferred, which allows you
    * to use the following syntax.
    *
    * {{{
    * consumerResource[F].using(settings)
    * }}}
    */
  def consumerResource[F[_], K, V](settings: ConsumerSettings[F, K, V])(
    implicit F: Async[F], unsafeRun: UnsafeRun[F]
  ): Resource[F, KafkaConsumer[F, K, V]] =
    KafkaConsumer.consumerResource(settings)

  /**
    * Alternative version of `consumerResource` where the `F[_]` is
    * specified explicitly, and where the key and value type can
    * be inferred from the [[ConsumerSettings]]. This allows you
    * to use the following syntax.
    *
    * {{{
    * consumerResource[F].using(settings)
    * }}}
    */
  def consumerResource[F[_]](implicit F: Async[F]): ConsumerResource[F] =
    new ConsumerResource[F](F)

  /**
    * Creates a new [[KafkaConsumer]] in the `Stream` context,
    * using the specified [[ConsumerSettings]]. Note that there
    * is another version where `F[_]` is specified explicitly and
    * the key and value type can be inferred, which allows you
    * to use the following syntax.
    *
    * {{{
    * consumerStream[F].using(settings)
    * }}}
    */
  def consumerStream[F[_], K, V](settings: ConsumerSettings[F, K, V])(
    implicit F: Async[F], ur: UnsafeRun[F]
  ): Stream[F, KafkaConsumer[F, K, V]] =
    Stream.resource(consumerResource(settings))

  /**
    * Alternative version of `consumerStream` where the `F[_]` is
    * specified explicitly, and where the key and value type can
    * be inferred from the [[ConsumerSettings]]. This allows you
    * to use the following syntax.
    *
    * {{{
    * consumerStream[F].using(settings)
    * }}}
    */
  def consumerStream[F[_]](implicit F: Async[F]): ConsumerStream[F] =
    new ConsumerStream[F](F)

  /**
    * Creates a new [[KafkaProducer]] in the `Resource` context,
    * using the specified [[ProducerSettings]]. Note that there
    * is another version where `F[_]` is specified explicitly and
    * the key and value type can be inferred, which allows you
    * to use the following syntax.
    *
    * {{{
    * producerResource[F].using(settings)
    * }}}
    */
  def producerResource[F[_], K, V](settings: ProducerSettings[F, K, V])(
    implicit F: Async[F]
  ): Resource[F, KafkaProducer.Metrics[F, K, V]] =
    KafkaProducer.resource(settings)

  /**
    * Alternative version of `producerResource` where the `F[_]` is
    * specified explicitly, and where the key and value type can
    * be inferred from the [[ProducerSettings]]. This allows you
    * to use the following syntax.
    *
    * {{{
    * producerResource[F].using(settings)
    * }}}
    */
  def producerResource[F[_]](implicit F: Async[F]): ProducerResource[F] =
    new ProducerResource(F)

  /**
    * Creates a new [[KafkaProducer]] in the `Stream` context,
    * using the specified [[ProducerSettings]]. Note that there
    * is another version where `F[_]` is specified explicitly and
    * the key and value type can be inferred, which allows you
    * to use the following syntax.
    *
    * {{{
    * producerStream[F].using(settings)
    * }}}
    */
  def producerStream[F[_], K, V](settings: ProducerSettings[F, K, V])(
    implicit F: Async[F]
  ): Stream[F, KafkaProducer.Metrics[F, K, V]] =
    Stream.resource(producerResource(settings))

  /**
    * Alternative version of `producerStream` where the `F[_]` is
    * specified explicitly, and where the key and value type can
    * be inferred from the [[ProducerSettings]]. This allows you
    * to use the following syntax.
    *
    * {{{
    * producerStream[F].using(settings)
    * }}}
    */
  def producerStream[F[_]](implicit F: Async[F]): ProducerStream[F] =
    new ProducerStream[F](F)

  /**
    * Creates a new [[TransactionalKafkaProducer]] in the `Resource` context,
    * using the specified [[TransactionalProducerSettings]]. Note that there
    * is another version where `F[_]` is specified explicitly and the key and
    * value type can be inferred, which allows you to use the following syntax.
    *
    * {{{
    * transactionalProducerResource[F].using(settings)
    * }}}
    */
  def transactionalProducerResource[F[_], K, V](
    settings: TransactionalProducerSettings[F, K, V]
  )(
    implicit F: Async[F]
  ): Resource[F, TransactionalKafkaProducer[F, K, V]] =
    TransactionalKafkaProducer.resource(settings)

  /**
    * Alternative version of `transactionalProducerResource` where the `F[_]`
    * is specified explicitly, and where the key and value type can be
    * inferred from the [[TransactionalProducerSettings]]. This allows
    * you to use the following syntax.
    *
    * {{{
    * transactionalProducerResource[F].using(settings)
    * }}}
    */
  def transactionalProducerResource[F[_]](
    implicit F: Async[F]
  ): TransactionalProducerResource[F] =
    new TransactionalProducerResource(F)

  /**
    * Creates a new [[TransactionalKafkaProducer]] in the `Stream` context,
    * using the specified [[TransactionalProducerSettings]]. Note that there
    * is another version where `F[_]` is specified explicitly and the key and
    * value type can be inferred, which allows you to use the following syntax.
    *
    * {{{
    * transactionalProducerStream[F].using(settings)
    * }}}
    */
  def transactionalProducerStream[F[_], K, V](
    settings: TransactionalProducerSettings[F, K, V]
  )(
    implicit F: Async[F]
  ): Stream[F, TransactionalKafkaProducer[F, K, V]] =
    Stream.resource(transactionalProducerResource(settings))

  /**
    * Alternative version of `transactionalProducerStream` where the `F[_]`
    * is specified explicitly, and where the key and value type can be
    * inferred from the [[TransactionalProducerSettings]]. This allows
    * you to use the following syntax.
    *
    * {{{
    * transactionalProducerStream[F].using(settings)
    * }}}
    */
  def transactionalProducerStream[F[_]](
    implicit F: Async[F]
  ): TransactionalProducerStream[F] =
    new TransactionalProducerStream(F)
}

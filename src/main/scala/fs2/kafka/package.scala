/*
 * Copyright 2018-2019 OVO Energy Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fs2

import java.util.concurrent.{Executors, ThreadFactory}

import cats.Applicative
import cats.effect._
import cats.syntax.traverse._

import scala.concurrent.ExecutionContext
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
    * Commits offsets in batches determined by the `Chunk`s of the
    * underlying `Stream`. If you want more explicit control over
    * how batches are created, instead use [[commitBatchChunk]].<br>
    * <br>
    * If your [[CommittableOffset]]s are wrapped in an effect `F[_]`,
    * like the produce effect from [[KafkaProducer.produce]], then
    * there is a [[commitBatchF]] function for that instead.
    *
    * @see [[commitBatchWithin]] for committing offset batches every `n`
    *      offsets or time window of length `d`, whichever happens first
    */
  def commitBatch[F[_]](
    implicit F: Applicative[F]
  ): Pipe[F, CommittableOffset[F], Unit] =
    _.chunks.through(commitBatchChunk)

  /**
    * Commits offsets in batches determined by the `Chunk`s of the
    * underlying `Stream`. If you want more explicit control over
    * how batches are created, instead use [[commitBatchChunkF]].<br>
    * <br>
    * Note that in order to enable offset commits in batches when also
    * producing records, you can use [[KafkaProducer.produce]] and
    * keep the [[CommittableOffset]] as passthrough value.<br>
    * <br>
    * If your [[CommittableOffset]]s are not wrapped in an effect `F[_]`,
    * like the produce effect from `produce`, then there is a
    * [[commitBatch]] function for that instead.
    *
    * @see [[commitBatchWithinF]] for committing offset batches every `n`
    *      offsets or time window of length `d`, whichever happens first
    */
  def commitBatchF[F[_]](
    implicit F: Applicative[F]
  ): Pipe[F, F[CommittableOffset[F]], Unit] =
    _.chunks.through(commitBatchChunkF)

  /**
    * Commits offsets in batches determined by the `Chunks` of the
    * underlying `Stream`. If you want more explicit control over
    * how batches are created, you can instead make use of
    * [[commitBatchChunkOption]].<br>
    * <br>
    * The offsets are wrapped in `Option` and only present offsets will
    * be committed. This is particularly useful when a consumed message
    * results in producing multiple messages, and an offset should only
    * be committed once all of the messages have been produced.<br>
    * <br>
    * If your [[CommittableOffset]]s are wrapped in an effect `F[_]`,
    * like the produce effect from [[KafkaProducer.produce]], then
    * there is a [[commitBatchOptionF]] function for that instead.
    *
    * @see [[commitBatchOptionWithin]] for committing offset batches every
    *     `n` offsets or time window of length `d`, whichever happens first
    */
  def commitBatchOption[F[_]](
    implicit F: Applicative[F]
  ): Pipe[F, Option[CommittableOffset[F]], Unit] =
    _.chunks.through(commitBatchChunkOption)

  /**
    * Commits offsets in batches determined by the `Chunks` of the
    * underlying `Stream`. If you want more explicit control over
    * how batches are created, you can instead make use of
    * [[commitBatchChunkOptionF]].<br>
    * <br>
    * The offsets are wrapped in `Option` and only present offsets will
    * be committed. This is particularly useful when a consumed message
    * results in producing multiple messages, and an offset should only
    * be committed once all of the messages have been produced.<br>
    * <br>
    * Note that in order to enable offset commits in batches when also
    * producing records, you can use [[KafkaProducer.produce]] and
    * keep the [[CommittableOffset]] as passthrough value.<br>
    * <br>
    * If your [[CommittableOffset]]s are not wrapped in an effect `F[_]`,
    * like the produce effect from `produce`, then there is a
    * [[commitBatchOption]] function for that instead.
    *
    * @see [[commitBatchOptionWithinF]] for committing offset batches every
    *     `n` offsets or time window of length `d`, whichever happens first
    */
  def commitBatchOptionF[F[_]](
    implicit F: Applicative[F]
  ): Pipe[F, F[Option[CommittableOffset[F]]], Unit] =
    _.chunks.through(commitBatchChunkOptionF)

  /**
    * Commits offsets in batches determined by `Chunk`s. This allows
    * you to explicitly control how offset batches are created. If you
    * want to use the underlying `Chunk`s of the `Stream`, simply
    * use [[commitBatch]] instead.<br>
    * <br>
    * If your [[CommittableOffset]]s are wrapped in an effect `F[_]`,
    * like the produce effect from [[KafkaProducer.produce]], then
    * there is a [[commitBatchChunkF]] function for that instead.
    *
    * @see [[commitBatchWithin]] for committing offset batches every `n`
    *      offsets or time window of length `d`, whichever happens first
    */
  def commitBatchChunk[F[_]](
    implicit F: Applicative[F]
  ): Pipe[F, Chunk[CommittableOffset[F]], Unit] =
    _.evalMap(CommittableOffsetBatch.fromFoldable(_).commit)

  /**
    * Commits offsets in batches determined by `Chunk`s. This allows
    * you to explicitly control how offset batches are created. If you
    * want to use the underlying `Chunk`s of the `Stream`, simply
    * use [[commitBatchF]] instead.<br>
    * <br>
    * Note that in order to enable offset commits in batches when also
    * producing records, you can use [[KafkaProducer.produce]] and
    * keep the [[CommittableOffset]] as passthrough value.<br>
    * <br>
    * If your [[CommittableOffset]]s are not wrapped in an effect `F[_]`,
    * like the produce effect from `produce`, then there is a
    * [[commitBatchChunk]] function for that instead.
    *
    * @see [[commitBatchWithinF]] for committing offset batches every `n`
    *      offsets or time window of length `d`, whichever happens first
    */
  def commitBatchChunkF[F[_]](
    implicit F: Applicative[F]
  ): Pipe[F, Chunk[F[CommittableOffset[F]]], Unit] =
    _.evalMap(_.sequence).through(commitBatchChunk)

  /**
    * Commits offsets in batches determined by `Chunk`s. This allows
    * you to explicitly control how offset batches are created. If you
    * want to use the underlying `Chunk`s of the `Stream`, simply
    * use [[commitBatchOption]] instead.<br>
    * <br>
    * The offsets are wrapped in `Option` and only present offsets will
    * be committed. This is particularly useful when a consumed message
    * results in producing multiple messages, and an offset should only
    * be committed once all of the messages have been produced.<br>
    * <br>
    * If your [[CommittableOffset]]s are wrapped in an effect `F[_]`,
    * like the produce effect from [[KafkaProducer.produce]], then
    * there is a [[commitBatchChunkOptionF]] for that instead.
    *
    * @see [[commitBatchOptionWithin]] for committing offset batches every
    *     `n` offsets or time window of length `d`, whichever happens first
    */
  def commitBatchChunkOption[F[_]](
    implicit F: Applicative[F]
  ): Pipe[F, Chunk[Option[CommittableOffset[F]]], Unit] =
    _.evalMap(CommittableOffsetBatch.fromFoldableOption(_).commit)

  /**
    * Commits offsets in batches determined by `Chunk`s. This allows
    * you to explicitly control how offset batches are created. If you
    * want to use the underlying `Chunk`s of the `Stream`, simply
    * use [[commitBatchOptionF]] instead.<br>
    * <br>
    * The offsets are wrapped in `Option` and only present offsets will
    * be committed. This is particularly useful when a consumed message
    * results in producing multiple messages, and an offset should only
    * be committed once all of the messages have been produced.<br>
    * <br>
    * Note that in order to enable offset commits in batches when also
    * producing records, you can use [[KafkaProducer.produce]] and
    * keep the [[CommittableOffset]] as passthrough value.<br>
    * <br>
    * If your [[CommittableOffset]]s are not wrapped in an effect `F[_]`,
    * like the produce effect from `produce`, then there is a
    * [[commitBatchChunkOption]] function for that instead.
    *
    * @see [[commitBatchOptionWithinF]] for committing offset batches every
    *     `n` offsets or time window of length `d`, whichever happens first
    */
  def commitBatchChunkOptionF[F[_]](
    implicit F: Applicative[F]
  ): Pipe[F, Chunk[F[Option[CommittableOffset[F]]]], Unit] =
    _.evalMap(_.sequence).through(commitBatchChunkOption)

  /**
    * Commits offsets in batches of every `n` offsets or time window
    * of length `d`, whichever happens first. If there are no offsets
    * to commit within a time window, no attempt will be made to commit
    * offsets for that time window.<br>
    * <br>
    * If your [[CommittableOffset]]s are wrapped in an effect `F[_]`,
    * like the produce effect from [[KafkaProducer.produce]], then
    * there is a [[commitBatchWithinF]] function for that instead.
    *
    * @see [[commitBatch]] for using the underlying `Chunk`s of
    *      the `Stream` as offset commit batches
    * @see [[commitBatchChunk]] for committing offset batches with
    *      explicit control over how offset batches are determined
    */
  def commitBatchWithin[F[_]](n: Int, d: FiniteDuration)(
    implicit F: Concurrent[F],
    timer: Timer[F]
  ): Pipe[F, CommittableOffset[F], Unit] =
    _.groupWithin(n, d).through(commitBatchChunk)

  /**
    * Commits offsets in batches of every `n` offsets or time window
    * of length `d`, whichever happens first. If there are no offsets
    * to commit within a time window, no attempt will be made to commit
    * offsets for that time window.<br>
    * <br>
    * Note that in order to enable offset commits in batches when also
    * producing records, you can use [[KafkaProducer.produce]] and
    * keep the [[CommittableOffset]] as passthrough value.<br>
    * <br>
    * If your [[CommittableOffset]]s are not wrapped in an effect `F[_]`,
    * like the produce effect from `produce`, then there is a
    * [[commitBatchWithin]] function for that instead.
    *
    * @see [[commitBatchF]] for using the underlying `Chunk`s of
    *      the `Stream` as offset commit batches
    * @see [[commitBatchChunkF]] for committing offset batches with
    *      explicit control over how offset batches are determined
    */
  def commitBatchWithinF[F[_]](n: Int, d: FiniteDuration)(
    implicit F: Concurrent[F],
    timer: Timer[F]
  ): Pipe[F, F[CommittableOffset[F]], Unit] =
    _.groupWithin(n, d).through(commitBatchChunkF)

  /**
    * Commits offsets in batches of every `n` offsets or time window
    * of length `d`, whichever happens first. If there are no offsets
    * to commit within a time window, no attempt will be made to commit
    * offsets for that time window.<br>
    * <br>
    * The offsets are wrapped in `Option` and only present offsets will
    * be committed. This is particularly useful when a consumed message
    * results in producing multiple messages, and an offset should only
    * be committed once all of the messages have been produced.<br>
    * <br>
    * If your [[CommittableOffset]]s are wrapped in an effect `F[_]`,
    * like the produce effect from [[KafkaProducer.produce]], then
    * there is a [[commitBatchOptionWithinF]] for that instead.
    *
    * @see [[commitBatchOption]] for using the underlying `Chunk`s of
    *      the `Stream` as offset commit batches
    * @see [[commitBatchChunkOption]] for committing offset batches with
    *      explicit control over how offset batches are determined
    */
  def commitBatchOptionWithin[F[_]](n: Int, d: FiniteDuration)(
    implicit F: Concurrent[F],
    timer: Timer[F]
  ): Pipe[F, Option[CommittableOffset[F]], Unit] =
    _.groupWithin(n, d).through(commitBatchChunkOption)

  /**
    * Commits offsets in batches of every `n` offsets or time window
    * of length `d`, whichever happens first. If there are no offsets
    * to commit within a time window, no attempt will be made to commit
    * offsets for that time window.<br>
    * <br>
    * The offsets are wrapped in `Option` and only present offsets will
    * be committed. This is particularly useful when a consumed message
    * results in producing multiple messages, and an offset should only
    * be committed once all of the messages have been produced.<br>
    * <br>
    * Note that in order to enable offset commits in batches when also
    * producing records, you can use [[KafkaProducer.produce]] and
    * keep the [[CommittableOffset]] as passthrough value.<br>
    * <br>
    * If your [[CommittableOffset]]s are not wrapped in an effect `F[_]`,
    * like the produce effect from `produce`, then there is a
    * [[commitBatchOptionWithin]] function for that instead.
    *
    * @see [[commitBatchOptionF]] for using the underlying `Chunk`s of
    *      the `Stream` as offset commit batches
    * @see [[commitBatchChunkOptionF]] for committing offset batches with
    *      explicit control over how offset batches are determined
    */
  def commitBatchOptionWithinF[F[_]](n: Int, d: FiniteDuration)(
    implicit F: Concurrent[F],
    timer: Timer[F]
  ): Pipe[F, F[Option[CommittableOffset[F]]], Unit] =
    _.groupWithin(n, d).through(commitBatchChunkOptionF)

  /**
    * Creates a new [[KafkaAdminClient]] in the `Resource` context,
    * using the specified [[AdminClientSettings]]. If working in a
    * `Stream` context, you might prefer [[adminClientStream]].
    */
  def adminClientResource[F[_]](settings: AdminClientSettings[F])(
    implicit F: Concurrent[F]
  ): Resource[F, KafkaAdminClient[F]] =
    KafkaAdminClient.resource(settings)

  /**
    * Creates a new [[KafkaAdminClient]] in the `Stream` context,
    * using the specified [[AdminClientSettings]]. If you're not
    * working in a `Stream` context, you might instead prefer to
    * use the [[adminClientResource]] function.
    */
  def adminClientStream[F[_]](settings: AdminClientSettings[F])(
    implicit F: Concurrent[F]
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
    implicit F: ConcurrentEffect[F],
    context: ContextShift[F],
    timer: Timer[F]
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
  def consumerResource[F[_]](implicit F: ConcurrentEffect[F]): ConsumerResource[F] =
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
    implicit F: ConcurrentEffect[F],
    context: ContextShift[F],
    timer: Timer[F]
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
  def consumerStream[F[_]](implicit F: ConcurrentEffect[F]): ConsumerStream[F] =
    new ConsumerStream[F](F)

  /**
    * Creates a new `ExecutionContext` backed by a single thread.
    * This is suitable for use with a single `KafkaConsumer`, and
    * is required to be set when creating [[ConsumerSettings]].<br>
    * <br>
    * If you already have an `ExecutionContext` for blocking code,
    * then you might prefer to use that over explicitly creating
    * one with this function.<br>
    * <br>
    * The thread created by this function will be of type daemon,
    * and the `Resource` context will automatically shutdown the
    * underlying `Executor` as part of finalization.<br>
    * <br>
    * You might prefer `consumerExecutionContextStream`, which is
    * returning a `Stream` instead of `Resource`. For convenience
    * when working together with `Stream`s.
    */
  def consumerExecutionContextResource[F[_]](
    implicit F: Sync[F]
  ): Resource[F, ExecutionContext] =
    consumerExecutionContextResource(1)

  /**
    * Creates a new `ExecutionContext` backed by the specified number
    * of `threads`. This is suitable for use with the same number of
    * `KafkaConsumer`s, and is required to be set when creating a
    * [[ConsumerSettings]] instance.<br>
    * <br>
    * If you already have an `ExecutionContext` for blocking code,
    * then you might prefer to use that over explicitly creating
    * one with this function.<br>
    * <br>
    * The threads created by this function will be of type daemon,
    * and the `Resource` context will automatically shutdown the
    * underlying `Executor` as part of finalization.<br>
    * <br>
    * You might prefer `consumerExecutionContextStream`, which is
    * returning a `Stream` instead of `Resource`. For convenience
    * when working together with `Stream`s.
    */
  def consumerExecutionContextResource[F[_]](threads: Int)(
    implicit F: Sync[F]
  ): Resource[F, ExecutionContext] =
    executionContextResource("fs2-kafka-consumer", threads)

  /**
    * Creates a new `ExecutionContext` backed by a single thread.
    * This is suitable for use with a single `KafkaProducer`, and
    * is required to be set when creating [[ProducerSettings]].<br>
    * <br>
    * If you already have an `ExecutionContext` for blocking code,
    * then you might prefer to use that over explicitly creating
    * one with this function.<br>
    * <br>
    * The thread created by this function will be of type daemon,
    * and the `Resource` context will automatically shutdown the
    * underlying `Executor` as part of finalization.<br>
    * <br>
    * You might prefer `producerExecutionContextStream`, which is
    * returning a `Stream` instead of `Resource`. For convenience
    * when working together with `Stream`s.
    */
  def producerExecutionContextResource[F[_]](
    implicit F: Sync[F]
  ): Resource[F, ExecutionContext] =
    producerExecutionContextResource(1)

  /**
    * Creates a new `ExecutionContext` backed by the specified number
    * of `threads`. This is suitable for use with the same number of
    * `KafkaProducer`s, and is required to be set when creating a
    * [[ProducerSettings]] instance.<br>
    * <br>
    * If you already have an `ExecutionContext` for blocking code,
    * then you might prefer to use that over explicitly creating
    * one with this function.<br>
    * <br>
    * The threads created by this function will be of type daemon,
    * and the `Resource` context will automatically shutdown the
    * underlying `Executor` as part of finalization.<br>
    * <br>
    * You might prefer `producerExecutionContextStream`, which is
    * returning a `Stream` instead of `Resource`. For convenience
    * when working together with `Stream`s.
    */
  def producerExecutionContextResource[F[_]](threads: Int)(
    implicit F: Sync[F]
  ): Resource[F, ExecutionContext] =
    executionContextResource("fs2-kafka-producer", threads)

  private[this] def executionContextResource[F[_]](
    name: String,
    threads: Int
  )(implicit F: Sync[F]): Resource[F, ExecutionContext] =
    Resource
      .make {
        F.delay {
          Executors.newFixedThreadPool(
            threads,
            new ThreadFactory {
              override def newThread(runnable: Runnable): Thread = {
                val thread = new Thread(runnable)
                thread.setName(s"$name-${thread.getId}")
                thread.setDaemon(true)
                thread
              }
            }
          )
        }
      }(executor => F.delay(executor.shutdown()))
      .map(ExecutionContext.fromExecutor)

  /**
    * Like `consumerExecutionContextResource`, but returns a `Stream`
    * rather than a `Resource`. This is for convenience when working
    * together with `Stream`s.
    */
  def consumerExecutionContextStream[F[_]](
    implicit F: Sync[F]
  ): Stream[F, ExecutionContext] =
    Stream.resource(consumerExecutionContextResource)

  /**
    * Like `consumerExecutionContextResource`, but returns a `Stream`
    * rather than a `Resource`. This is for convenience when working
    * together with `Stream`s.
    */
  def consumerExecutionContextStream[F[_]](threads: Int)(
    implicit F: Sync[F]
  ): Stream[F, ExecutionContext] =
    Stream.resource(consumerExecutionContextResource(threads))

  /**
    * Like `producerExecutionContextResource`, but returns a `Stream`
    * rather than a `Resource`. This is for convenience when working
    * together with `Stream`s.
    */
  def producerExecutionContextStream[F[_]](
    implicit F: Sync[F]
  ): Stream[F, ExecutionContext] =
    Stream.resource(producerExecutionContextResource)

  /**
    * Like `producerExecutionContextResource`, but returns a `Stream`
    * rather than a `Resource`. This is for convenience when working
    * together with `Stream`s.
    */
  def producerExecutionContextStream[F[_]](threads: Int)(
    implicit F: Sync[F]
  ): Stream[F, ExecutionContext] =
    Stream.resource(producerExecutionContextResource(threads))

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
    implicit F: ConcurrentEffect[F],
    context: ContextShift[F]
  ): Resource[F, KafkaProducer[F, K, V]] =
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
  def producerResource[F[_]](implicit F: ConcurrentEffect[F]): ProducerResource[F] =
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
    implicit F: ConcurrentEffect[F],
    context: ContextShift[F]
  ): Stream[F, KafkaProducer[F, K, V]] =
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
  def producerStream[F[_]](implicit F: ConcurrentEffect[F]): ProducerStream[F] =
    new ProducerStream[F](F)

  /**
    * Creates a new [[TransactionalKafkaProducer]] in the `Resource` context,
    * using the specified [[ProducerSettings]]. Note that there is another
    * version where `F[_]` is specified explicitly and the key and value
    * type can be inferred, which allows you to use the following syntax.
    *
    * {{{
    * transactionalProducerResource[F].using(settings)
    * }}}
    */
  def transactionalProducerResource[F[_], K, V](
    settings: ProducerSettings[F, K, V]
  )(
    implicit F: ConcurrentEffect[F],
    context: ContextShift[F]
  ): Resource[F, TransactionalKafkaProducer[F, K, V]] =
    TransactionalKafkaProducer.resource(settings)

  /**
    * Alternative version of `transactionalProducerResource` where the `F[_]`
    * is specified explicitly, and where the key and value type can be
    * inferred from the [[ProducerSettings]]. This allows you to use
    * the following syntax.
    *
    * {{{
    * transactionalProducerResource[F].using(settings)
    * }}}
    */
  def transactionalProducerResource[F[_]](
    implicit F: ConcurrentEffect[F]
  ): TransactionalProducerResource[F] =
    new TransactionalProducerResource(F)

  /**
    * Creates a new [[TransactionalKafkaProducer]] in the `Stream` context,
    * using the specified [[ProducerSettings]]. Note that there is another
    * version where `F[_]` is specified explicitly and the key and value
    * type can be inferred, which allows you to use the following syntax.
    *
    * {{{
    * transactionalProducerStream[F].using(settings)
    * }}}
    */
  def transactionalProducerStream[F[_], K, V](
    settings: ProducerSettings[F, K, V]
  )(
    implicit F: ConcurrentEffect[F],
    context: ContextShift[F]
  ): Stream[F, TransactionalKafkaProducer[F, K, V]] =
    Stream.resource(transactionalProducerResource(settings))

  /**
    * Alternative version of `transactionalProducerStream` where the `F[_]`
    * is specified explicitly, and where the key and value type can be
    * inferred from the [[ProducerSettings]]. This allows you to use
    * the following syntax.
    *
    * {{{
    * transactionalProducerStream[F].using(settings)
    * }}}
    */
  def transactionalProducerStream[F[_]](
    implicit F: ConcurrentEffect[F]
  ): TransactionalProducerStream[F] =
    new TransactionalProducerStream(F)
}

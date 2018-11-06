/*
 * Copyright 2018 OVO Energy Ltd
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
import cats.syntax.functor._
import cats.syntax.traverse._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

package object kafka {

  /**
    * Commits offsets in batches determined by the `Chunk`s of the
    * underlying `Stream`. If you want more explicit control over
    * how batches are created, instead use [[commitBatchChunk]].<br>
    * <br>
    * If your [[CommittableOffset]]s are wrapped in an effect `F`,
    * like the produce effect from [[KafkaProducer.produceBatched]],
    * then there is a [[commitBatchF]] function for that instead.
    *
    * @see [[commitBatchWithin]] for committing offset batches every `n`
    *      offsets or time window of length `d`, whichever happens first
    */
  def commitBatch[F[_]](
    implicit F: Applicative[F]
  ): Sink[F, CommittableOffset[F]] =
    _.chunks.to(commitBatchChunk)

  /**
    * Commits offsets in batches determined by the `Chunk`s of the
    * underlying `Stream`. If you want more explicit control over
    * how batches are created, instead use [[commitBatchChunkF]].<br>
    * <br>
    * Note that in order to enable offset commits in batches when also
    * producing records, you can use [[KafkaProducer.produceBatched]]
    * and keep the [[CommittableOffset]] as passthrough value.<br>
    * <br>
    * If your [[CommittableOffset]]s are not wrapped in an effect `F`,
    * like the produce effect from `produceBatched`, then there is a
    * [[commitBatch]] function for that instead.
    *
    * @see [[commitBatchWithinF]] for committing offset batches every `n`
    *      offsets or time window of length `d`, whichever happens first
    */
  def commitBatchF[F[_]](
    implicit F: Applicative[F]
  ): Sink[F, F[CommittableOffset[F]]] =
    _.chunks.to(commitBatchChunkF)

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
    * If your [[CommittableOffset]]s are wrapped in an effect `F`,
    * like the produce effect from [[KafkaProducer.produceBatched]],
    * then there is a [[commitBatchOptionF]] function for that instead.
    *
    * @see [[commitBatchOptionWithin]] for committing offset batches every
    *     `n` offsets or time window of length `d`, whichever happens first
    */
  def commitBatchOption[F[_]](
    implicit F: Applicative[F]
  ): Sink[F, Option[CommittableOffset[F]]] =
    _.chunks.to(commitBatchChunkOption)

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
    * producing records, you can use [[KafkaProducer.produceBatched]]
    * and keep the [[CommittableOffset]] as passthrough value.<br>
    * <br>
    * If your [[CommittableOffset]]s are not wrapped in an effect `F`,
    * like the produce effect from `produceBatched`, then there is a
    * [[commitBatchOption]] function for that instead.
    *
    * @see [[commitBatchOptionWithinF]] for committing offset batches every
    *     `n` offsets or time window of length `d`, whichever happens first
    */
  def commitBatchOptionF[F[_]](
    implicit F: Applicative[F]
  ): Sink[F, F[Option[CommittableOffset[F]]]] =
    _.chunks.to(commitBatchChunkOptionF)

  /**
    * Commits offsets in batches determined by `Chunk`s. This allows
    * you to explicitly control how offset batches are created. If you
    * want to use the underlying `Chunk`s of the `Stream`, simply
    * use [[commitBatch]] instead.<br>
    * <br>
    * If your [[CommittableOffset]]s are wrapped in an effect `F`,
    * like the produce effect from [[KafkaProducer.produceBatched]],
    * then there is a [[commitBatchChunkF]] function for that instead.
    *
    * @see [[commitBatchWithin]] for committing offset batches every `n`
    *      offsets or time window of length `d`, whichever happens first
    */
  def commitBatchChunk[F[_]](
    implicit F: Applicative[F]
  ): Sink[F, Chunk[CommittableOffset[F]]] =
    _.evalMap(CommittableOffsetBatch.fromFoldable(_).commit)

  /**
    * Commits offsets in batches determined by `Chunk`s. This allows
    * you to explicitly control how offset batches are created. If you
    * want to use the underlying `Chunk`s of the `Stream`, simply
    * use [[commitBatchF]] instead.<br>
    * <br>
    * Note that in order to enable offset commits in batches when also
    * producing records, you can use [[KafkaProducer.produceBatched]]
    * and keep the [[CommittableOffset]] as passthrough value.<br>
    * <br>
    * If your [[CommittableOffset]]s are not wrapped in an effect `F`,
    * like the produce effect from `produceBatched`, then there is a
    * [[commitBatchChunk]] function for that instead.
    *
    * @see [[commitBatchWithinF]] for committing offset batches every `n`
    *      offsets or time window of length `d`, whichever happens first
    */
  def commitBatchChunkF[F[_]](
    implicit F: Applicative[F]
  ): Sink[F, Chunk[F[CommittableOffset[F]]]] =
    _.evalMap(_.sequence).to(commitBatchChunk)

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
    * If your [[CommittableOffset]]s are wrapped in an effect `F`,
    * like the produce effect from [[KafkaProducer.produceBatched]],
    * then there is a [[commitBatchChunkOptionF]] for that instead.
    *
    * @see [[commitBatchOptionWithin]] for committing offset batches every
    *     `n` offsets or time window of length `d`, whichever happens first
    */
  def commitBatchChunkOption[F[_]](
    implicit F: Applicative[F]
  ): Sink[F, Chunk[Option[CommittableOffset[F]]]] =
    _.evalMap {
      _.foldLeft(CommittableOffsetBatch.empty[F]) {
        case (batch, Some(offset)) => batch.updated(offset)
        case (batch, None)         => batch
      }.commit
    }

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
    * producing records, you can use [[KafkaProducer.produceBatched]]
    * and keep the [[CommittableOffset]] as passthrough value.<br>
    * <br>
    * If your [[CommittableOffset]]s are not wrapped in an effect `F`,
    * like the produce effect from `produceBatched`, then there is a
    * [[commitBatchChunkOption]] function for that instead.
    *
    * @see [[commitBatchOptionWithinF]] for committing offset batches every
    *     `n` offsets or time window of length `d`, whichever happens first
    */
  def commitBatchChunkOptionF[F[_]](
    implicit F: Applicative[F]
  ): Sink[F, Chunk[F[Option[CommittableOffset[F]]]]] =
    _.evalMap(_.sequence).to(commitBatchChunkOption)

  /**
    * Commits offsets in batches of every `n` offsets or time window
    * of length `d`, whichever happens first. If there are no offsets
    * to commit within a time window, no attempt will be made to commit
    * offsets for that time window.<br>
    * <br>
    * If your [[CommittableOffset]]s are wrapped in an effect `F`,
    * like the produce effect from [[KafkaProducer.produceBatched]],
    * then there is a [[commitBatchWithinF]] function for that instead.
    *
    * @see [[commitBatch]] for using the underlying `Chunk`s of
    *      the `Stream` as offset commit batches
    * @see [[commitBatchChunk]] for committing offset batches with
    *      explicit control over how offset batches are determined
    */
  def commitBatchWithin[F[_]](n: Int, d: FiniteDuration)(
    implicit F: Concurrent[F],
    timer: Timer[F]
  ): Sink[F, CommittableOffset[F]] =
    _.groupWithin(n, d).to(commitBatchChunk)

  /**
    * Commits offsets in batches of every `n` offsets or time window
    * of length `d`, whichever happens first. If there are no offsets
    * to commit within a time window, no attempt will be made to commit
    * offsets for that time window.<br>
    * <br>
    * Note that in order to enable offset commits in batches when also
    * producing records, you can use [[KafkaProducer.produceBatched]]
    * and keep the [[CommittableOffset]] as passthrough value.<br>
    * <br>
    * If your [[CommittableOffset]]s are not wrapped in an effect `F`,
    * like the produce effect from `produceBatched`, then there is a
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
  ): Sink[F, F[CommittableOffset[F]]] =
    _.groupWithin(n, d).to(commitBatchChunkF)

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
    * If your [[CommittableOffset]]s are wrapped in an effect `F`,
    * like the produce effect from [[KafkaProducer.produceBatched]],
    * then there is a [[commitBatchOptionWithinF]] for that instead.
    *
    * @see [[commitBatchOption]] for using the underlying `Chunk`s of
    *      the `Stream` as offset commit batches
    * @see [[commitBatchChunkOption]] for committing offset batches with
    *      explicit control over how offset batches are determined
    */
  def commitBatchOptionWithin[F[_]](n: Int, d: FiniteDuration)(
    implicit F: Concurrent[F],
    timer: Timer[F]
  ): Sink[F, Option[CommittableOffset[F]]] =
    _.groupWithin(n, d).to(commitBatchChunkOption)

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
    * producing records, you can use [[KafkaProducer.produceBatched]]
    * and keep the [[CommittableOffset]] as passthrough value.<br>
    * <br>
    * If your [[CommittableOffset]]s are not wrapped in an effect `F`,
    * like the produce effect from `produceBatched`, then there is a
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
  ): Sink[F, F[Option[CommittableOffset[F]]]] =
    _.groupWithin(n, d).to(commitBatchChunkOptionF)

  def consumerResource[F[_], K, V](settings: ConsumerSettings[K, V])(
    implicit F: ConcurrentEffect[F],
    context: ContextShift[F],
    timer: Timer[F]
  ): Resource[F, KafkaConsumer[F, K, V]] =
    KafkaConsumer.consumerResource(settings)

  def consumerResource[F[_]]: ConsumerResource[F] =
    new ConsumerResource[F]

  def consumerStream[F[_], K, V](settings: ConsumerSettings[K, V])(
    implicit F: ConcurrentEffect[F],
    context: ContextShift[F],
    timer: Timer[F]
  ): Stream[F, KafkaConsumer[F, K, V]] =
    Stream.resource(consumerResource(settings))

  def consumerStream[F[_]]: ConsumerStream[F] =
    new ConsumerStream[F]

  def consumerExecutionContextResource[F[_]](implicit F: Sync[F]): Resource[F, ExecutionContext] =
    Resource
      .make {
        F.delay {
          Executors.newSingleThreadExecutor(new ThreadFactory {
            override def newThread(runnable: Runnable): Thread = {
              val thread = new Thread(runnable)
              thread.setName(s"fs2-kafka-consumer-${thread.getId}")
              thread.setDaemon(true)
              thread
            }
          })
        }
      }(executor => F.delay(executor.shutdown()))
      .map(ExecutionContext.fromExecutor)

  def consumerExecutionContextStream[F[_]](implicit F: Sync[F]): Stream[F, ExecutionContext] =
    Stream.resource(consumerExecutionContextResource[F])

  def producerResource[F[_], K, V](settings: ProducerSettings[K, V])(
    implicit F: ConcurrentEffect[F]
  ): Resource[F, KafkaProducer[F, K, V]] =
    KafkaProducer.producerResource(settings)

  def producerResource[F[_]]: ProducerResource[F] =
    new ProducerResource[F]

  def producerStream[F[_], K, V](settings: ProducerSettings[K, V])(
    implicit F: ConcurrentEffect[F]
  ): Stream[F, KafkaProducer[F, K, V]] =
    Stream.resource(producerResource(settings))

  def producerStream[F[_]]: ProducerStream[F] =
    new ProducerStream[F]
}

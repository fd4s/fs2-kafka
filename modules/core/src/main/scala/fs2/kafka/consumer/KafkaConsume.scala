/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.consumer

import fs2._
import fs2.kafka.CommittableConsumerRecord
import org.apache.kafka.common.TopicPartition

trait KafkaConsume[F[_], K, V] {

  /**
    * Alias for `partitionedStream.parJoinUnbounded`.
    * See [[partitionedStream]] for more information.
    *
    * @note you have to first use `subscribe` to subscribe the consumer
    *       before using this `Stream`. If you forgot to subscribe, there
    *       will be a [[NotSubscribedException]] raised in the `Stream`.
    */
  def stream: Stream[F, CommittableConsumerRecord[F, K, V]]

  /**
    * `Stream` where the elements themselves are `Stream`s which continually
    * request records for a single partition. These `Stream`s will have to be
    * processed in parallel, using `parJoin` or `parJoinUnbounded`. Note that
    * when using `parJoin(n)` and `n` is smaller than the number of currently
    * assigned partitions, then there will be assigned partitions which won't
    * be processed. For that reason, prefer `parJoinUnbounded` and the actual
    * limit will be the number of assigned partitions.<br>
    * <br>
    * If you do not want to process all partitions in parallel, then you
    * can use [[stream]] instead, where records for all partitions are in
    * a single `Stream`.
    *
    * @note you have to first use `subscribe` to subscribe the consumer
    *       before using this `Stream`. If you forgot to subscribe, there
    *       will be a [[NotSubscribedException]] raised in the `Stream`.
    */
  def partitionedStream: Stream[F, Stream[F, CommittableConsumerRecord[F, K, V]]]

  /**
    * Same as partitionedStream but topic 'Stream' is tupled with 'TopicPartition'
    */
  def signedPartitionedStream: Stream[F, (TopicPartition, Stream[F, CommittableConsumerRecord[F, K, V]])]
  /**
    * `Stream` where each element contains a current assignment. The current
    * assignment is the `Map`, where keys is a `TopicPartition`, and values are
    * streams with records for a particular `TopicPartition`.<br>
    * <br>
    * New assignments will be received on each rebalance. On rebalance,
    * Kafka revoke all previously assigned partitions, and after that assigned
    * new partitions all at once. `partitionsMapStream` reflects this process
    * in a streaming manner.<br>
    * <br>
    * Note, that partition streams for revoked partitions will
    * be closed after the new assignment comes.<br>
    * <br>
    * This is the most generic `Stream` method. If you don't need such control,
    * consider using `partitionedStream` or `stream` methods.
    * They are both based on a `partitionsMapStream`.
    *
    * @note you have to first use `subscribe` to subscribe the consumer
    *       before using this `Stream`. If you forgot to subscribe, there
    *       will be a [[NotSubscribedException]] raised in the `Stream`.
    * @see [[stream]]
    * @see [[partitionedStream]]
    */
  def partitionsMapStream
    : Stream[F, Map[TopicPartition, Stream[F, CommittableConsumerRecord[F, K, V]]]]

  /**
    * Stops consuming new messages from Kafka.
    * This method could be used to implement a graceful shutdown.<br>
    * <br>
    * This method has a few effects:
    * 1. After this call no more data will be fetched from Kafka through the `poll` method.
    * 2. All currently running streams will continue to run until all in-flight messages will be processed.
    *    It means that streams will be completed when all fetched messages will be processed.<br>
    * <br>
    * If some of the [[stream]] methods will be called after [[stopConsuming]] call,
    * these methods will return empty streams.<br>
    * <br>
    * More than one call of [[stopConsuming]] will have no effect.
    */
  def stopConsuming: F[Unit]
}

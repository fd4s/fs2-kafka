/*
 * Copyright 2018-2022 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.consumer

import org.apache.kafka.common.TopicPartition
import cats.Foldable
import scala.concurrent.duration.FiniteDuration

trait KafkaOffsets[F[_]] {
  /**
    * Overrides the fetch offsets that the consumer will use when reading the
    * next record. If this API is invoked for the same partition more than once,
    * the latest offset will be used. Note that you may lose data if this API is
    * arbitrarily used in the middle of consumption to reset the fetch offsets.
    */
  def seek(partition: TopicPartition, offset: Long): F[Unit]

  /**
    * Seeks to the first offset for each currently assigned partition.
    * This is equivalent to using `seekToBeginning` with an empty set
    * of partitions.<br>
    * <br>
    * Note that this seek evaluates lazily, and only on the next call
    * to `poll` or `position`.
    */
  def seekToBeginning: F[Unit] = seekToBeginning(List.empty[TopicPartition])

  /**
    * Seeks to the first offset for each of the specified partitions.
    * If no partitions are provided, seeks to the first offset for
    * all currently assigned partitions.<br>
    * <br>
    * Note that this seek evaluates lazily, and only on the next call
    * to `poll` or `position`.
    */
  def seekToBeginning[G[_]: Foldable](partitions: G[TopicPartition]): F[Unit]

  /**
    * Seeks to the last offset for each currently assigned partition.
    * This is equivalent to using `seekToEnd` with an empty set of
    * partitions.<br>
    * <br>
    * Note that this seek evaluates lazily, and only on the next call
    * to `poll` or `position`.
    */
  def seekToEnd: F[Unit] = seekToEnd(List.empty[TopicPartition])

  /**
    * Seeks to the last offset for each of the specified partitions.
    * If no partitions are provided, seeks to the last offset for
    * all currently assigned partitions.<br>
    * <br>
    * Note that this seek evaluates lazily, and only on the next call
    * to `poll` or `position`.
    */
  def seekToEnd[G[_]: Foldable](partitions: G[TopicPartition]): F[Unit]

  /**
    * Returns the offset of the next record that will be fetched.<br>
    * <br>
    * Timeout is determined by `default.api.timeout.ms`, which
    * is set using [[ConsumerSettings#withDefaultApiTimeout]].
    */
  def position(partition: TopicPartition): F[Long]

  /**
    * Returns the offset of the next record that will be fetched.
    */
  def position(partition: TopicPartition, timeout: FiniteDuration): F[Long]
}

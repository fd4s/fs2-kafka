/*
 * Copyright 2018-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.consumer

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

import scala.concurrent.duration.FiniteDuration

trait KafkaOffsetsV2[F[_]] extends KafkaOffsets[F] {

  /**
    * Returns the last committed offsets for the given partitions.
    */
  def committed(partitions: Set[TopicPartition]): F[Map[TopicPartition, OffsetAndMetadata]]

  /**
    * Returns the last committed offsets for the given partitions.<br>
    * <br>
    * Timeout is determined by `default.api.timeout.ms`, which
    * is set using [[ConsumerSettings#withDefaultApiTimeout]].
    */
  def committed(
    partitions: Set[TopicPartition],
    timeout: FiniteDuration
  ): F[Map[TopicPartition, OffsetAndMetadata]]
}

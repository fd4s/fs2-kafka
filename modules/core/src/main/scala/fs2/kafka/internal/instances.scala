/*
 * Copyright 2018-2020 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.instances.int._
import cats.instances.long._
import cats.instances.string._
import cats.instances.tuple._
import cats.syntax.show._
import cats.{Order, Show}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition

private[kafka] object instances {
  implicit val offsetAndMetadataOrder: Order[OffsetAndMetadata] =
    Order.by(oam => (oam.offset, oam.metadata))

  implicit val offsetAndMetadataOrdering: Ordering[OffsetAndMetadata] =
    Order[OffsetAndMetadata].toOrdering

  implicit val offsetAndMetadataShow: Show[OffsetAndMetadata] =
    Show.show { oam =>
      if (oam.metadata.nonEmpty)
        show"(${oam.offset}, ${oam.metadata})"
      else oam.offset.show
    }

  implicit val recordMetadataShow: Show[RecordMetadata] =
    Show.show(rm => show"${rm.topic}-${rm.partition}@${rm.offset}")

  implicit val topicPartitionOrder: Order[TopicPartition] =
    Order.by(tp => (tp.topic, tp.partition))

  implicit val topicPartitionOrdering: Ordering[TopicPartition] =
    Order[TopicPartition].toOrdering

  implicit val topicPartitionShow: Show[TopicPartition] =
    Show.show(tp => show"${tp.topic}-${tp.partition}")
}

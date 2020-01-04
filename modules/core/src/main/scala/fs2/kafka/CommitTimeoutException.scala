/*
 * Copyright 2018-2020 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.instances.list._
import cats.instances.string._
import cats.syntax.show._
import fs2.kafka.internal.instances._
import fs2.kafka.internal.syntax._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.{KafkaException, TopicPartition}

import scala.concurrent.duration.FiniteDuration

/**
  * [[CommitTimeoutException]] indicates that offset commit took longer
  * than the configured [[ConsumerSettings#commitTimeout]]. The timeout
  * and offsets are included in the exception message.
  */
sealed abstract class CommitTimeoutException(
  timeout: FiniteDuration,
  offsets: Map[TopicPartition, OffsetAndMetadata]
) extends KafkaException({
      offsets.toList.sorted.mkStringAppend {
        case (append, (tp, oam)) =>
          append(tp.show)
          append(" -> ")
          append(oam.show)
      }(
        start = s"offset commit timeout after $timeout for offsets: ",
        sep = ", ",
        end = ""
      )
    })

private[kafka] object CommitTimeoutException {
  def apply(
    timeout: FiniteDuration,
    offsets: Map[TopicPartition, OffsetAndMetadata]
  ): CommitTimeoutException =
    new CommitTimeoutException(timeout, offsets) {
      override def toString: String =
        show"fs2.kafka.CommitTimeoutException: $getMessage"
    }
}

package fs2.kafka

import cats.instances.string._
import cats.syntax.show._
import fs2.kafka.internal.instances._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.{KafkaException, TopicPartition}

import scala.concurrent.duration.FiniteDuration

sealed abstract class CommitTimeoutException(
  timeout: FiniteDuration,
  offsets: Map[TopicPartition, OffsetAndMetadata]
) extends KafkaException({
      val builder = new StringBuilder(s"offset commit timeout after $timeout for offsets: ")
      val sorted = offsets.toList.sorted
      var first = true

      sorted.foreach {
        case (tp, oam) =>
          if (first) first = false
          else builder.append(", ")

          builder.append(tp.show).append(" -> ").append(oam.show)
      }

      builder.toString
    }) {

  override def toString: String =
    show"fs2.kafka.CommitTimeoutException: $getMessage"
}

object CommitTimeoutException {
  private[kafka] def apply(
    timeout: FiniteDuration,
    offsets: Map[TopicPartition, OffsetAndMetadata]
  ): CommitTimeoutException =
    new CommitTimeoutException(timeout, offsets) {}
}

package fs2.kafka

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.{KafkaException, TopicPartition}

import scala.concurrent.duration.FiniteDuration

final class CommitTimeoutException(
  timeout: FiniteDuration,
  offsets: Map[TopicPartition, OffsetAndMetadata]
) extends KafkaException(s"Offset commit timeout after $timeout: $offsets")

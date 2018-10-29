package fs2.kafka

import org.apache.kafka.common.KafkaException

sealed abstract class NotSubscribedException
    extends KafkaException("consumer is not subscribed to any topics") {

  override def toString: String =
    s"fs2.kafka.NotSubscribedException: $getMessage"
}

case object NotSubscribedException extends NotSubscribedException

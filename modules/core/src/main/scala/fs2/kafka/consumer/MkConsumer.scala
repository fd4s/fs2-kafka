package fs2.kafka.consumer

import cats.effect.Sync
import fs2.kafka.KafkaByteConsumer
import fs2.kafka.internal.converters.collection._
import org.apache.kafka.common.serialization.ByteArrayDeserializer

trait MkConsumer[F[_]] {
  def make(properties: Map[String, String]): F[KafkaByteConsumer]
}

object MkConsumer {
  implicit def mkConsumerForSync[F[_]](implicit F: Sync[F]): MkConsumer[F] =
    properties =>
      F.delay {
        val byteArrayDeserializer = new ByteArrayDeserializer
        new org.apache.kafka.clients.consumer.KafkaConsumer(
          (properties: Map[String, AnyRef]).asJava,
          byteArrayDeserializer,
          byteArrayDeserializer
        )
      }
}

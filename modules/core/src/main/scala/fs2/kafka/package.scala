/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2

import cats.effect._
import scala.concurrent.duration.FiniteDuration

package object kafka {
  type Id[+A] = A

  /** Alias for Java Kafka `Consumer[Array[Byte], Array[Byte]]`. */
  type JavaByteConsumer =
    org.apache.kafka.clients.consumer.Consumer[Array[Byte], Array[Byte]]

  @deprecated("use JavaByteConsumer", "1.3.0")
  type KafkaByteConsumer = JavaByteConsumer

  /** Alias for Java Kafka `Producer[Array[Byte], Array[Byte]]`. */
  type JavaByteProducer =
    org.apache.kafka.clients.producer.Producer[Array[Byte], Array[Byte]]

  @deprecated("use JavaByteProducer", "1.3.0")
  type KafkaByteProducer = JavaByteProducer

  /** Alias for Java Kafka `Deserializer[A]`. */
  type JavaDeserializer[A] =
    org.apache.kafka.common.serialization.Deserializer[A]

  @deprecated("use JavaDeserializer", "1.3.0")
  type KafkaDeserializer[A] = JavaDeserializer[A]

  /** Alias for Java Kafka `Serializer[A]`. */
  type JavaSerializer[A] =
    org.apache.kafka.common.serialization.Serializer[A]

  @deprecated("use JavaSerializer", "1.3.0")
  type KafkaSerializer[A] = JavaSerializer[A]

  /** Alias for Java Kafka `Header`. */
  type JavaHeader =
    org.apache.kafka.common.header.Header

  @deprecated("use JavaHeader", "1.3.0")
  type KafkaHeader = JavaHeader

  /** Alias for Java Kafka `Headers`. */
  type JavaHeaders =
    org.apache.kafka.common.header.Headers

  @deprecated("use JavaHeaders", "1.3.0")
  type KafkaHeaders = JavaHeaders

  /** Alias for Java Kafka `ConsumerRecords[Array[Byte], Array[Byte]]`. */
  type JavaByteConsumerRecords =
    org.apache.kafka.clients.consumer.ConsumerRecords[Array[Byte], Array[Byte]]

  @deprecated("use JavaByteConsumerRecords", "1.3.0")
  type KafkaByteConsumerRecords = JavaByteConsumerRecords

  /** Alias for Java Kafka `ConsumerRecord[Array[Byte], Array[Byte]]`. */
  type JavaByteConsumerRecord =
    org.apache.kafka.clients.consumer.ConsumerRecord[Array[Byte], Array[Byte]]

  @deprecated("use JavaByteConsumerRecord", "1.3.0")
  type KafkaByteConsumerRecord = JavaByteConsumerRecord

  /** Alias for Java Kafka `ProducerRecord[Array[Byte], Array[Byte]]`. */
  type JavaByteProducerRecord =
    org.apache.kafka.clients.producer.ProducerRecord[Array[Byte], Array[Byte]]

  @deprecated("use JavaByteProducerRecord", "1.3.0")
  type KafkaByteProducerRecord = JavaByteProducerRecord

  /**
    * Commits offsets in batches of every `n` offsets or time window
    * of length `d`, whichever happens first. If there are no offsets
    * to commit within a time window, no attempt will be made to commit
    * offsets for that time window.
    */
  def commitBatchWithin[F[_]](n: Int, d: FiniteDuration)(
    implicit F: Concurrent[F],
    timer: Timer[F]
  ): Pipe[F, CommittableOffset[F], Unit] =
    _.groupWithin(n, d).evalMap(CommittableOffsetBatch.fromFoldable(_).commit)
}

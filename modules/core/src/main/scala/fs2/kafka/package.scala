/*
 * Copyright 2018-2022 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2

import cats.effect._
import scala.concurrent.duration.FiniteDuration

package object kafka {
  type Id[+A] = A

  /** Alias for Java Kafka `Consumer[Array[Byte], Array[Byte]]`. */
  type KafkaByteConsumer =
    org.apache.kafka.clients.consumer.Consumer[Array[Byte], Array[Byte]]

  /** Alias for Java Kafka `Producer[Array[Byte], Array[Byte]]`. */
  type KafkaByteProducer =
    org.apache.kafka.clients.producer.Producer[Array[Byte], Array[Byte]]

  /** Alias for Java Kafka `Deserializer[A]`. */
  type KafkaDeserializer[A] =
    org.apache.kafka.common.serialization.Deserializer[A]

  /** Alias for Java Kafka `Serializer[A]`. */
  type KafkaSerializer[A] =
    org.apache.kafka.common.serialization.Serializer[A]

  /** Alias for Java Kafka `Header`. */
  type KafkaHeader =
    org.apache.kafka.common.header.Header

  /** Alias for Java Kafka `Headers`. */
  type KafkaHeaders =
    org.apache.kafka.common.header.Headers

  /** Alias for Java Kafka `ConsumerRecords[Array[Byte], Array[Byte]]`. */
  type KafkaByteConsumerRecords =
    org.apache.kafka.clients.consumer.ConsumerRecords[Array[Byte], Array[Byte]]

  /** Alias for Java Kafka `ConsumerRecord[Array[Byte], Array[Byte]]`. */
  type KafkaByteConsumerRecord =
    org.apache.kafka.clients.consumer.ConsumerRecord[Array[Byte], Array[Byte]]

  /** Alias for Java Kafka `ProducerRecord[Array[Byte], Array[Byte]]`. */
  type KafkaByteProducerRecord =
    org.apache.kafka.clients.producer.ProducerRecord[Array[Byte], Array[Byte]]

  /**
    * Commits offsets in batches of every `n` offsets or time window
    * of length `d`, whichever happens first. If there are no offsets
    * to commit within a time window, no attempt will be made to commit
    * offsets for that time window.
    */
  def commitBatchWithin[F[_]](n: Int, d: FiniteDuration)(
    implicit F: Temporal[F]
  ): Pipe[F, CommittableOffset[F], Unit] =
    _.groupWithin(n, d).evalMap(CommittableOffsetBatch.fromFoldable(_).commit)

  type Serializer[F[_], A] = GenSerializer[KeyOrValue, F, A]
  type KeySerializer[F[_], A] = GenSerializer[Key, F, A]
  type ValueSerializer[F[_], A] = GenSerializer[Value, F, A]
  val Serializer: GenSerializer.type = GenSerializer

  type Deserializer[F[_], A] = GenDeserializer[KeyOrValue, F, A]
  type KeyDeserializer[F[_], A] = GenDeserializer[Key, F, A]
  type ValueDeserializer[F[_], A] = GenDeserializer[Value, F, A]
  val Deserializer: GenDeserializer.type = GenDeserializer
}

package kafka {

  /** Phantom types to indicate whether a [[Serializer]]/[[Deserializer]] if for keys, values, or both
    */
  sealed trait KeyOrValue
  sealed trait Key extends KeyOrValue
  sealed trait Value extends KeyOrValue
}

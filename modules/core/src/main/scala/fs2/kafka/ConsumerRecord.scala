/*
 * Copyright 2018-2019 OVO Energy Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fs2.kafka

import cats.{Apply, Show}
import cats.implicits._
import fs2.kafka.internal.syntax._
import org.apache.kafka.clients.consumer.ConsumerRecord.{NULL_SIZE, NO_TIMESTAMP}
import org.apache.kafka.common.record.TimestampType.{CREATE_TIME, LOG_APPEND_TIME}

/**
  * [[ConsumerRecord]] represents a record which has been
  * consumed from Kafka. At the very least, this includes
  * a key of type `K`, value of type `V`, and the topic,
  * partition, and offset of the consumed record.<br>
  * <br>
  * To create a new instance, use [[ConsumerRecord#apply]]
  */
sealed abstract class ConsumerRecord[+K, +V] {

  /** The topic from which the record has been consumed. */
  def topic: String

  /** The partition from which the record has been consumed. */
  def partition: Int

  /** The offset from which the record has been consumed. */
  def offset: Long

  /** The record key. */
  def key: K

  /** The record value. */
  def value: V

  /** The record headers. */
  def headers: Headers

  /** The record timestamp. */
  def timestamp: Timestamp

  /** The serialized key size if available. */
  def serializedKeySize: Option[Int]

  /** The serialized value size if available. */
  def serializedValueSize: Option[Int]

  /** The leader epoch if available. */
  def leaderEpoch: Option[Int]

  /**
    * Creates a new [[ConsumerRecord]] instance with the
    * specified headers as the headers for the record.
    */
  def withHeaders(headers: Headers): ConsumerRecord[K, V]

  /**
    * Creates a new [[ConsumerRecord]] instance with the
    * specified timestamp as the timestamp for the record.
    */
  def withTimestamp(timestamp: Timestamp): ConsumerRecord[K, V]

  /**
    * Creates a new [[ConsumerRecord]] instance with the
    * specified key size as the key size for the record.
    */
  def withSerializedKeySize(serializedKeySize: Int): ConsumerRecord[K, V]

  /**
    * Creates a new [[ConsumerRecord]] instance with the
    * specified value size as the size for the record.
    */
  def withSerializedValueSize(serializedValueSize: Int): ConsumerRecord[K, V]

  /**
    * Creates a new [[ConsumerRecord]] instance with the
    * specified leader epoch as the epoch for the record.
    */
  def withLeaderEpoch(leaderEpoch: Int): ConsumerRecord[K, V]
}

object ConsumerRecord {
  private[this] final case class ConsumerRecordImpl[+K, +V](
    override val topic: String,
    override val partition: Int,
    override val offset: Long,
    override val key: K,
    override val value: V,
    override val headers: Headers,
    override val timestamp: Timestamp,
    override val serializedKeySize: Option[Int],
    override val serializedValueSize: Option[Int],
    override val leaderEpoch: Option[Int]
  ) extends ConsumerRecord[K, V] {
    override def withHeaders(headers: Headers): ConsumerRecord[K, V] =
      copy(headers = headers)

    override def withTimestamp(timestamp: Timestamp): ConsumerRecord[K, V] =
      copy(timestamp = timestamp)

    override def withSerializedKeySize(serializedKeySize: Int): ConsumerRecord[K, V] =
      copy(serializedKeySize = Some(serializedKeySize))

    override def withSerializedValueSize(serializedValueSize: Int): ConsumerRecord[K, V] =
      copy(serializedValueSize = Some(serializedValueSize))

    override def withLeaderEpoch(leaderEpoch: Int): ConsumerRecord[K, V] =
      copy(leaderEpoch = Some(leaderEpoch))

    override def toString: String = {
      val b = new java.lang.StringBuilder("ConsumerRecord(")
      b.append("topic = ").append(topic)
      b.append(", partition = ").append(partition)
      b.append(", offset = ").append(offset)
      b.append(", key = ").append(key)
      b.append(", value = ").append(value)
      if (headers.nonEmpty)
        b.append(", headers = ").append(headers)
      if (timestamp.nonEmpty)
        b.append(", timestamp = ").append(timestamp)
      if (serializedKeySize.nonEmpty)
        b.append(", serializedKeySize = ").append(serializedKeySize.get)
      if (serializedValueSize.nonEmpty)
        b.append(", serializedValueSize = ").append(serializedValueSize.get)
      if (leaderEpoch.nonEmpty)
        b.append(", leaderEpoch = ").append(leaderEpoch.get)
      b.append(")").toString
    }
  }

  /**
    * Creates a new [[ConsumerRecord]] instance using the
    * specified key and value, and the topic, partition,
    * and offset of the record.
    */
  def apply[K, V](
    topic: String,
    partition: Int,
    offset: Long,
    key: K,
    value: V
  ): ConsumerRecord[K, V] =
    ConsumerRecordImpl(
      topic = topic,
      partition = partition,
      offset = offset,
      key = key,
      value = value,
      headers = Headers.empty,
      timestamp = Timestamp.none,
      serializedKeySize = None,
      serializedValueSize = None,
      leaderEpoch = None
    )

  def unapply[K, V](cr: ConsumerRecord[K, V]): Some[(String, Int, Long, K, V, Headers)] =
    Some((cr.topic, cr.partition, cr.offset, cr.key, cr.value, cr.headers))

  private[this] def deserializeFromBytes[F[_], K, V](
    record: KafkaByteConsumerRecord,
    headers: Headers,
    keyDeserializer: Deserializer[F, K],
    valueDeserializer: Deserializer[F, V]
  )(implicit F: Apply[F]): F[(K, V)] = {
    val key = keyDeserializer.deserialize(record.topic, headers, record.key)
    val value = valueDeserializer.deserialize(record.topic, headers, record.value)
    key.product(value)
  }

  private[kafka] def fromJava[F[_], K, V](
    record: KafkaByteConsumerRecord,
    keyDeserializer: Deserializer[F, K],
    valueDeserializer: Deserializer[F, V]
  )(implicit F: Apply[F]): F[ConsumerRecord[K, V]] = {
    val headers = record.headers.asScala
    deserializeFromBytes(record, headers, keyDeserializer, valueDeserializer).map {
      case (key, value) =>
        ConsumerRecordImpl(
          topic = record.topic,
          partition = record.partition,
          offset = record.offset,
          key = key,
          value = value,
          headers = headers,
          timestamp = {
            if (record.timestamp != NO_TIMESTAMP) {
              record.timestampType match {
                case CREATE_TIME     => Timestamp.createTime(record.timestamp)
                case LOG_APPEND_TIME => Timestamp.logAppendTime(record.timestamp)
                case _               => Timestamp.unknownTime(record.timestamp)
              }
            } else {
              Timestamp.none
            }
          },
          serializedKeySize =
            if (record.serializedKeySize != NULL_SIZE)
              Some(record.serializedKeySize)
            else None,
          serializedValueSize =
            if (record.serializedValueSize != NULL_SIZE)
              Some(record.serializedValueSize)
            else None,
          leaderEpoch =
            if (record.leaderEpoch.isPresent)
              Some(record.leaderEpoch.get)
            else None
        )
    }
  }

  implicit def consumerRecordShow[K, V](
    implicit
    K: Show[K],
    V: Show[V]
  ): Show[ConsumerRecord[K, V]] = Show.show { record =>
    val b = new java.lang.StringBuilder("ConsumerRecord(")
    b.append("topic = ").append(record.topic)
    b.append(", partition = ").append(record.partition)
    b.append(", offset = ").append(record.offset)
    b.append(", key = ").append(record.key.show)
    b.append(", value = ").append(record.value.show)
    if (record.headers.nonEmpty)
      b.append(", headers = ").append(record.headers)
    if (record.timestamp.nonEmpty)
      b.append(", timestamp = ").append(record.timestamp)
    if (record.serializedKeySize.nonEmpty)
      b.append(", serializedKeySize = ").append(record.serializedKeySize.get)
    if (record.serializedValueSize.nonEmpty)
      b.append(", serializedValueSize = ").append(record.serializedValueSize.get)
    if (record.leaderEpoch.nonEmpty)
      b.append(", leaderEpoch = ").append(record.leaderEpoch.get)
    b.append(")").toString
  }
}

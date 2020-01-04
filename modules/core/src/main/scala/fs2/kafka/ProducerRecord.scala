/*
 * Copyright 2018-2020 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.Show
import cats.syntax.show._

/**
  * [[ProducerRecord]] represents a record which can be produced
  * to Kafka. At the very least, this includes a key of type `K`,
  * a value of type `V`, and to which topic the record should be
  * produced. The partition, timestamp, and headers can be set
  * by using the [[withPartition]], [[withTimestamp]], and
  * [[withHeaders]] functions, respectively.<br>
  * <br>
  * To create a new instance, use [[ProducerRecord#apply]].
  */
sealed abstract class ProducerRecord[+K, +V] {

  /** The topic to which the record should be produced. */
  def topic: String

  /** The partition to which the record should be produced. */
  def partition: Option[Int]

  /** The timestamp for when the record was produced. */
  def timestamp: Option[Long]

  /** The record key. */
  def key: K

  /** The record value. */
  def value: V

  /** The record headers. */
  def headers: Headers

  /**
    * Creates a new [[ProducerRecord]] instance with the
    * specified partition as the partition to which the
    * record should be produced.
    */
  def withPartition(partition: Int): ProducerRecord[K, V]

  /**
    * Creates a new [[ProducerRecord]] instance with the
    * specified timestamp as the timestamp for when the
    * record was produced.
    */
  def withTimestamp(timestamp: Long): ProducerRecord[K, V]

  /**
    * Creates a new [[ProducerRecord]] instance with the
    * specified headers as the headers for the record.
    */
  def withHeaders(headers: Headers): ProducerRecord[K, V]
}

object ProducerRecord {
  private[this] final case class ProducerRecordImpl[+K, +V](
    override val topic: String,
    override val partition: Option[Int],
    override val timestamp: Option[Long],
    override val key: K,
    override val value: V,
    override val headers: Headers
  ) extends ProducerRecord[K, V] {
    override def withPartition(partition: Int): ProducerRecord[K, V] =
      copy(partition = Some(partition))

    override def withTimestamp(timestamp: Long): ProducerRecord[K, V] =
      copy(timestamp = Some(timestamp))

    override def withHeaders(headers: Headers): ProducerRecord[K, V] =
      copy(headers = headers)

    override def toString: String = {
      val b = new java.lang.StringBuilder("ProducerRecord(")
      b.append("topic = ").append(topic)
      if (partition.nonEmpty) b.append(", partition = ").append(partition.get)
      if (timestamp.nonEmpty) b.append(", timestamp = ").append(timestamp.get)
      b.append(", key = ").append(key)
      b.append(", value = ").append(value)
      if (headers.nonEmpty) b.append(", headers = ").append(headers)
      b.append(")").toString
    }
  }

  /**
    * Creates a new [[ProducerRecord]] instance using the
    * specified key and value, and the topic to which the
    * record should be produced.
    */
  def apply[K, V](
    topic: String,
    key: K,
    value: V
  ): ProducerRecord[K, V] =
    ProducerRecordImpl(
      topic = topic,
      partition = None,
      timestamp = None,
      key = key,
      value = value,
      headers = Headers.empty
    )

  implicit def producerRecordShow[K, V](
    implicit
    K: Show[K],
    V: Show[V]
  ): Show[ProducerRecord[K, V]] = Show.show { record =>
    val b = new java.lang.StringBuilder("ProducerRecord(")
    b.append("topic = ").append(record.topic)
    if (record.partition.nonEmpty) b.append(", partition = ").append(record.partition.get)
    if (record.timestamp.nonEmpty) b.append(", timestamp = ").append(record.timestamp.get)
    b.append(", key = ").append(record.key.show)
    b.append(", value = ").append(record.value.show)
    if (record.headers.nonEmpty) b.append(", headers = ").append(record.headers)
    b.append(")").toString
  }
}

/*
 * Copyright 2018 OVO Energy Ltd
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

package fs2.kafka.internal

import java.util

import cats.instances.int._
import cats.instances.long._
import cats.instances.string._
import cats.instances.tuple._
import cats.syntax.show._
import cats.{Order, Show}
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.{Header, Headers}
import org.apache.kafka.common.record.TimestampType

private[kafka] object instances {
  implicit def consumerRecordShow[K, V](
    implicit
    K: Show[K],
    V: Show[V]
  ): Show[ConsumerRecord[K, V]] = Show.show { cr =>
    show"ConsumerRecord(topic = ${cr.topic}, partition = ${cr.partition}, offset = ${cr.offset}, ${cr.timestampType} = ${cr.timestamp}, serialized key size = ${cr.serializedKeySize}, serialized value size = ${cr.serializedValueSize}, headers = ${cr.headers}, key = ${cr.key}, value = ${cr.value})"
  }

  implicit val headerShow: Show[Header] =
    Show.show(h => show"${h.key} -> ${util.Arrays.toString(h.value)}")

  implicit val headerOrder: Order[Header] =
    Order.by(_.key)

  implicit val headerOrdering: Ordering[Header] =
    Order[Header].toOrdering

  implicit val headersShow: Show[Headers] =
    Show.show { hs =>
      val headers = hs.toArray
      if (headers.isEmpty) "Headers(<empty>)"
      else headers.sorted.map(_.show).mkString("Headers(", ", ", ")")
    }

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

  implicit def producerRecordShow[K, V](
    implicit
    K: Show[K],
    V: Show[V]
  ): Show[ProducerRecord[K, V]] = Show.show { r =>
    val headers = if (r.headers != null) r.headers.show else "null"
    val key = if (r.key != null) r.key.show else "null"
    val value = if (r.value != null) r.value.show else "null"
    val timestamp = if (r.timestamp != null) (r.timestamp: Long).show else "null"
    show"ProducerRecord(topic = ${r.topic}, partition = ${(r.partition: Int).show}, headers = $headers, key = $key, value = $value, timestamp = $timestamp)"
  }

  implicit val recordMetadataShow: Show[RecordMetadata] =
    Show.show(rm => show"${rm.topic}-${rm.partition}@${rm.offset}")

  implicit val timestampTypeShow: Show[TimestampType] =
    Show.show(_.name)

  implicit val topicPartitionOrder: Order[TopicPartition] =
    Order.by(tp => (tp.topic, tp.partition))

  implicit val topicPartitionOrdering: Ordering[TopicPartition] =
    Order[TopicPartition].toOrdering

  implicit val topicPartitionShow: Show[TopicPartition] =
    Show.show(tp => show"${tp.topic}-${tp.partition}")
}

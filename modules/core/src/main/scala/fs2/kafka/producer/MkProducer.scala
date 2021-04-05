/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.producer

import cats.effect.Sync
import fs2.kafka.{KafkaByteProducer, ProducerSettings}
import org.apache.kafka.common.serialization.ByteArraySerializer
import fs2.kafka.internal.converters.collection._

trait MkProducer[F[_]] {
  def apply(settings: ProducerSettings[F, _, _]): F[KafkaByteProducer]
}

object MkProducer {
  implicit def mkProducerForSync[F[_]](implicit F: Sync[F]): MkProducer[F] =
    settings =>
      F.delay {
        val byteArraySerializer = new ByteArraySerializer
        new org.apache.kafka.clients.producer.KafkaProducer(
          (settings.properties: Map[String, AnyRef]).asJava,
          byteArraySerializer,
          byteArraySerializer
        )
      }
}

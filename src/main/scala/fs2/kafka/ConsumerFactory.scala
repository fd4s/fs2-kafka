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

package fs2.kafka

import cats.effect.Sync
import org.apache.kafka.clients.consumer.Consumer

import scala.collection.JavaConverters._

abstract class ConsumerFactory {
  def create[F[_], K, V](
    settings: ConsumerSettings[K, V]
  )(implicit F: Sync[F]): F[Consumer[K, V]]
}

object ConsumerFactory {
  val Default: ConsumerFactory =
    new ConsumerFactory {
      override def create[F[_], K, V](
        settings: ConsumerSettings[K, V]
      )(implicit F: Sync[F]): F[Consumer[K, V]] =
        F.delay {
          new org.apache.kafka.clients.consumer.KafkaConsumer(
            (settings.properties: Map[String, AnyRef]).asJava,
            settings.keyDeserializer,
            settings.valueDeserializer
          )
        }

      override def toString: String =
        "Default"
    }
}

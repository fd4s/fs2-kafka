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

/**
  * [[ConsumerFactory]] represents the ability to create a
  * new Kafka `Consumer` given [[ConsumerSettings]]. Normal
  * usage does not require a custom [[ConsumerFactory]], but
  * it can be useful for testing purposes.<br>
  * <br>
  * To create a new [[ConsumerFactory]], simply create a new
  * instance and implement the [[create]] function with the
  * desired `Consumer` behaviour. To use a custom instance
  * of [[ConsumerFactory]], you can simply set it with the
  * [[ConsumerSettings#withConsumerFactory]] function.<br>
  * <br>
  * [[ConsumerFactory#Default]] is the default instance, and
  * it creates a default `KafkaConsumer` instance from the
  * provided [[ConsumerSettings]].
  */
abstract class ConsumerFactory {
  def create[F[_], K, V](
    settings: ConsumerSettings[K, V]
  )(implicit F: Sync[F]): F[Consumer[K, V]]
}

object ConsumerFactory {

  /**
    * The default [[ConsumerFactory]] used in [[ConsumerSettings]]
    * unless a different one has been specified. Default instance
    * creates `KafkaConsumer` instances from provided settings.
    */
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

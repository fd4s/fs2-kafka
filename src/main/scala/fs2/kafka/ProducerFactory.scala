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
import org.apache.kafka.clients.producer.Producer

import scala.collection.JavaConverters._

/**
  * [[ProducerFactory]] represents the ability to create a
  * new Kafka `Producer` given [[ProducerSettings]]. Normal
  * usage does not require a custom [[ProducerFactory]], but
  * it can be useful for testing purposes. If you can instead
  * have a custom [[KafkaProducer]] for testing, then prefer
  * that over having a custom [[ProducerFactory]].<br>
  * <br>
  * To create a new [[ProducerFactory]], simply create a new
  * instance and implement the [[create]] function with the
  * desired `Producer` behaviour. To use a custom instance
  * of [[ProducerFactory]], you can simply set it with the
  * [[ProducerSettings#withProducerFactory]] function.<br>
  * <br>
  * [[ProducerFactory#Default]] is the default instance, and
  * it creates a default `KafkaProducer` instance from the
  * provided [[ProducerSettings]].
  */
abstract class ProducerFactory {
  def create[F[_], K, V](
    settings: ProducerSettings[K, V]
  )(implicit F: Sync[F]): F[Producer[K, V]]
}

object ProducerFactory {

  /**
    * The default [[ProducerFactory]] used in [[ProducerSettings]]
    * unless a different one has been specified. Default instance
    * creates `KafkaProducer` instances from provided settings.
    */
  val Default: ProducerFactory =
    new ProducerFactory {
      override def create[F[_], K, V](
        settings: ProducerSettings[K, V]
      )(implicit F: Sync[F]): F[Producer[K, V]] =
        F.delay {
          new org.apache.kafka.clients.producer.KafkaProducer(
            (settings.properties: Map[String, AnyRef]).asJava,
            settings.keySerializer,
            settings.valueSerializer
          )
        }

      override def toString: String =
        "Default"
    }
}

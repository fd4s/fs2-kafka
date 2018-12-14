/*
 * Copyright 2018 OVO Energy Limited
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
import org.apache.kafka.clients.admin.AdminClient

import scala.collection.JavaConverters._

/**
  * [[AdminClientFactory]] represents the ability to create a
  * new Kafka `AdminClient` given [[AdminClientSettings]]. We
  * normally do not need a custom [[AdminClientFactory]], but
  * it can be useful for testing purposes. If you can instead
  * have a custom trait or class with only the required parts
  * from [[KafkaAdminClient]] for testing, then prefer that.<br>
  * <br>
  * To create a new [[AdminClientFactory]], simply create a
  * new instance and implement the [[create]] function with
  * the desired behaviour. To use a custom instance, set it
  * with [[AdminClientSettings#withAdminClientFactory]].<br>
  * <br>
  * [[AdminClientFactory#Default]] is the default instance,
  * and it creates a default `AdminClient` instance from
  * the provided [[AdminClientSettings]].
  */
abstract class AdminClientFactory {
  def create[F[_]](
    settings: AdminClientSettings
  )(implicit F: Sync[F]): F[AdminClient]
}

object AdminClientFactory {

  /**
    * The default [[AdminClientFactory]] used in [[AdminClientSettings]]
    * unless a different instance has been specified. Default instance
    * creates `AdminClient` instances from provided settings.
    */
  val Default: AdminClientFactory =
    new AdminClientFactory {
      override def create[F[_]](
        settings: AdminClientSettings
      )(implicit F: Sync[F]): F[AdminClient] =
        F.delay {
          AdminClient.create {
            (settings.properties: Map[String, AnyRef]).asJava
          }
        }

      override def toString: String =
        "Default"
    }
}

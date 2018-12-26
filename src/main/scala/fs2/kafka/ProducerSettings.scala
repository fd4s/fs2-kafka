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

import cats.Show
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serializer

import scala.concurrent.duration._

/**
  * [[ProducerSettings]] contain settings necessary to create a [[KafkaProducer]].
  * At the very least, this includes a key serializer and a value serializer.<br>
  * <br>
  * Several convenience functions are provided so that you don't have to work with
  * `String` values and `ProducerConfig` for configuration. It's still possible to
  * specify `ProducerConfig` values with functions like [[withProperty]].<br>
  * <br>
  * [[ProducerSettings]] instances are immutable and all modification functions
  * return a new [[ProducerSettings]] instance.<br>
  * <br>
  * Use [[ProducerSettings#apply]] to create a new instance.
  */
sealed abstract class ProducerSettings[K, V] {

  /**
    * The `Serializer` to use for serializing record keys.
    */
  def keySerializer: Serializer[K]

  /**
    * The `Serializer` to use for serializing record values.
    */
  def valueSerializer: Serializer[V]

  /**
    * Properties which can be provided when creating a Java `KafkaProducer`
    * instance. Numerous functions in [[ProducerSettings]] add properties
    * here if the settings are used by the Java `KafkaProducer`.
    */
  def properties: Map[String, String]

  /**
    * Returns a new [[ProducerSettings]] instance with the specified
    * bootstrap servers. This is equivalent to setting the following
    * property using the [[withProperty]] function.
    *
    * {{{
    * ProducerConfig.BOOTSTRAP_SERVERS_CONFIG
    * }}}
    */
  def withBootstrapServers(bootstrapServers: String): ProducerSettings[K, V]

  /**
    * Returns a new [[ProducerSettings]] instance with the specified
    * acknowledgements. This is equivalent to setting the following
    * property using the [[withProperty]] function, except you can
    * specify it with [[Acks]] instead of a `String`.
    *
    * {{{
    * ProducerConfig.ACKS_CONFIG
    * }}}
    */
  def withAcks(acks: Acks): ProducerSettings[K, V]

  /**
    * Returns a new [[ProducerSettings]] instance with the specified
    * batch size. This is equivalent to setting the following property
    * using the [[withProperty]] function, except you can specify it
    * with an `Int` instead of a `String`.
    *
    * {{{
    * ProducerConfig.BATCH_SIZE_CONFIG
    * }}}
    */
  def withBatchSize(batchSize: Int): ProducerSettings[K, V]

  /**
    * Returns a new [[ProducerSettings]] instance with the specified
    * client id. This is equivalent to setting the following property
    * using the [[withProperty]] function.
    *
    * {{{
    * ProducerConfig.CLIENT_ID_CONFIG
    * }}}
    */
  def withClientId(clientId: String): ProducerSettings[K, V]

  /**
    * Returns a new [[ProducerSettings]] instance with the specified
    * retries. This is equivalent to setting the following property
    * using the [[withProperty]] function, except you can specify
    * it with an `Int` instead of a `String`.
    *
    * {{{
    * ProducerConfig.RETRIES_CONFIG
    * }}}
    */
  def withRetries(retries: Int): ProducerSettings[K, V]

  /**
    * Returns a new [[ProducerSettings]] instance with the specified
    * max in-flight requests per connection. This is equivalent to
    * setting the following property using the [[withProperty]]
    * function, except you can specify it with an `Int` instead
    * of a `String`.
    *
    * {{{
    * ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION
    * }}}
    */
  def withMaxInFlightRequestsPerConnection(
    maxInFlightRequestsPerConnection: Int
  ): ProducerSettings[K, V]

  /**
    * Returns a new [[ProducerSettings]] instance with the specified
    * idempotence setting. This is equivalent to setting the following
    * property using the [[withProperty]] function, except you can
    * specify it with a `Boolean` instead of a `String`.
    *
    * {{{
    * ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG
    * }}}
    */
  def withEnableIdempotence(enableIdempotence: Boolean): ProducerSettings[K, V]

  /**
    * Returns a new [[ProducerSettings]] instance with the specified
    * linger. This is equivalent to setting the following property
    * using the [[withProperty]] function, except you can specify
    * it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * ProducerConfig.LINGER_MS_CONFIG
    * }}}
    */
  def withLinger(linger: FiniteDuration): ProducerSettings[K, V]

  /**
    * Returns a new [[ProducerSettings]] instance with the specified
    * request timeout. This is equivalent to setting the following
    * property using the [[withProperty]] function, except you can
    * specify it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG
    * }}}
    */
  def withRequestTimeout(requestTimeout: FiniteDuration): ProducerSettings[K, V]

  /**
    * Returns a new [[ProducerSettings]] instance with the specified
    * delivery timeout. This is equivalent to setting the following
    * property using the [[withProperty]] function, except you can
    * specify it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG
    * }}}
    */
  def withDeliveryTimeout(deliveryTimeout: FiniteDuration): ProducerSettings[K, V]

  /**
    * Includes a property with the specified `key` and `value`.
    * The key should be one of the keys in `ProducerConfig`,
    * and the value should be a valid choice for the key.
    */
  def withProperty(key: String, value: String): ProducerSettings[K, V]

  /**
    * Includes the specified keys and values as properties. The
    * keys should be part of the `ProducerConfig` keys, and
    * the values should be valid choices for the keys.
    */
  def withProperties(properties: (String, String)*): ProducerSettings[K, V]

  /**
    * Includes the specified keys and values as properties. The
    * keys should be part of the `ProducerConfig` keys, and
    * the values should be valid choices for the keys.
    */
  def withProperties(properties: Map[String, String]): ProducerSettings[K, V]

  /**
    * The time to wait for the Java `KafkaProducer` to shutdown.<br>
    * <br>
    * The default value is 60 seconds.
    */
  def closeTimeout: FiniteDuration

  /**
    * Creates a new [[ProducerSettings]] with the specified [[closeTimeout]].
    */
  def withCloseTimeout(closeTimeout: FiniteDuration): ProducerSettings[K, V]

  /**
    * The [[ProducerFactory]] for creating the Java `Producer`.
    * The default is [[ProducerFactory#Default]]. Note that you
    * normally don't need to have a custom [[ProducerFactory]],
    * and you should instead prefer to create a custom trait or
    * class similar to [[KafkaProducer]] for testing purposes.
    */
  def producerFactory: ProducerFactory

  /**
    * Creates a new [[ProducerSettings]] with the specified
    * [[ProducerFactory]] as the [[producerFactory]] to use.
    * Note that under normal usage you don't need to have a
    * custom [[ProducerFactory]] instance. For testing, you
    * should prefer to create a custom trait or class
    * similar to [[KafkaProducer]].
    */
  def withProducerFactory(producerFactory: ProducerFactory): ProducerSettings[K, V]
}

object ProducerSettings {
  private[this] final case class ProducerSettingsImpl[K, V](
    override val keySerializer: Serializer[K],
    override val valueSerializer: Serializer[V],
    override val properties: Map[String, String],
    override val closeTimeout: FiniteDuration,
    override val producerFactory: ProducerFactory
  ) extends ProducerSettings[K, V] {
    override def withBootstrapServers(bootstrapServers: String): ProducerSettings[K, V] =
      withProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

    override def withAcks(acks: Acks): ProducerSettings[K, V] =
      withProperty(ProducerConfig.ACKS_CONFIG, acks match {
        case Acks.ZeroAcks => "0"
        case Acks.OneAcks  => "1"
        case Acks.AllAcks  => "all"
      })

    override def withBatchSize(batchSize: Int): ProducerSettings[K, V] =
      withProperty(ProducerConfig.BATCH_SIZE_CONFIG, batchSize.toString)

    override def withClientId(clientId: String): ProducerSettings[K, V] =
      withProperty(ProducerConfig.CLIENT_ID_CONFIG, clientId)

    override def withRetries(retries: Int): ProducerSettings[K, V] =
      withProperty(ProducerConfig.RETRIES_CONFIG, retries.toString)

    override def withMaxInFlightRequestsPerConnection(
      maxInFlightRequestsPerConnection: Int
    ): ProducerSettings[K, V] =
      withProperty(
        ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
        maxInFlightRequestsPerConnection.toString
      )

    override def withEnableIdempotence(enableIdempotence: Boolean): ProducerSettings[K, V] =
      withProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotence.toString)

    override def withLinger(linger: FiniteDuration): ProducerSettings[K, V] =
      withProperty(ProducerConfig.LINGER_MS_CONFIG, linger.toMillis.toString)

    override def withRequestTimeout(requestTimeout: FiniteDuration): ProducerSettings[K, V] =
      withProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout.toMillis.toString)

    override def withDeliveryTimeout(deliveryTimeout: FiniteDuration): ProducerSettings[K, V] =
      withProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeout.toMillis.toString)

    override def withProperty(key: String, value: String): ProducerSettings[K, V] =
      copy(properties = properties.updated(key, value))

    override def withProperties(properties: (String, String)*): ProducerSettings[K, V] =
      copy(properties = this.properties ++ properties.toMap)

    override def withProperties(properties: Map[String, String]): ProducerSettings[K, V] =
      copy(properties = this.properties ++ properties)

    override def withCloseTimeout(closeTimeout: FiniteDuration): ProducerSettings[K, V] =
      copy(closeTimeout = closeTimeout)

    override def withProducerFactory(producerFactory: ProducerFactory): ProducerSettings[K, V] =
      copy(producerFactory = producerFactory)

    override def toString: String =
      Show[ProducerSettings[K, V]].show(this)
  }

  /**
    * Creates a new [[ProducerSettings]] instance using the specified settings.
    */
  def apply[K, V](
    keySerializer: Serializer[K],
    valueSerializer: Serializer[V]
  ): ProducerSettings[K, V] =
    ProducerSettingsImpl(
      keySerializer = keySerializer,
      valueSerializer = valueSerializer,
      properties = Map.empty,
      closeTimeout = 60.seconds,
      producerFactory = ProducerFactory.Default
    )

  implicit def producerSettingsShow[K, V]: Show[ProducerSettings[K, V]] =
    Show.show { s =>
      s"ProducerSettings(closeTimeout = ${s.closeTimeout}, producerFactory = ${s.producerFactory})"
    }
}

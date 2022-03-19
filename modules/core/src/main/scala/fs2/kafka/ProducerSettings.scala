/*
 * Copyright 2018-2022 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.Show
import fs2.kafka.security.KafkaCredentialStore
import org.apache.kafka.clients.producer.ProducerConfig

import scala.concurrent.ExecutionContext
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
  * Use `ProducerSettings#apply` to create a new instance.
  */
sealed trait ProducerSettings {

  /**
    * A custom [[ExecutionContext]] to use for blocking Kafka operations.
    * If not provided, the default blocking ExecutionContext provided by
    * [[cats.effect.Sync]] will be used.
    */
  def customBlockingContext: Option[ExecutionContext]

  /**
    * Returns a new [[ProducerSettings]] instance with the
    * specified [[ExecutionContext]] to use for blocking operations.
    *
    * If not provided, the default blocking ExecutionContext provided by
    * [[cats.effect.Sync]] will be used. If in doubt, leave this unset.
    */
  def withCustomBlockingContext(ec: ExecutionContext): ProducerSettings

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
  def withBootstrapServers(bootstrapServers: String): ProducerSettings

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
  def withAcks(acks: Acks): ProducerSettings

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
  def withBatchSize(batchSize: Int): ProducerSettings

  /**
    * Returns a new [[ProducerSettings]] instance with the specified
    * client id. This is equivalent to setting the following property
    * using the [[withProperty]] function.
    *
    * {{{
    * ProducerConfig.CLIENT_ID_CONFIG
    * }}}
    */
  def withClientId(clientId: String): ProducerSettings

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
  def withRetries(retries: Int): ProducerSettings

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
  ): ProducerSettings

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
  def withEnableIdempotence(enableIdempotence: Boolean): ProducerSettings

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
  def withLinger(linger: FiniteDuration): ProducerSettings

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
  def withRequestTimeout(requestTimeout: FiniteDuration): ProducerSettings

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
  def withDeliveryTimeout(deliveryTimeout: FiniteDuration): ProducerSettings

  /**
    * Includes a property with the specified `key` and `value`.
    * The key should be one of the keys in `ProducerConfig`,
    * and the value should be a valid choice for the key.
    */
  def withProperty(key: String, value: String): ProducerSettings

  /**
    * Includes the specified keys and values as properties. The
    * keys should be part of the `ProducerConfig` keys, and
    * the values should be valid choices for the keys.
    */
  def withProperties(properties: (String, String)*): ProducerSettings

  /**
    * Includes the specified keys and values as properties. The
    * keys should be part of the `ProducerConfig` keys, and
    * the values should be valid choices for the keys.
    */
  def withProperties(properties: Map[String, String]): ProducerSettings

  /**
    * The time to wait for the Java `KafkaProducer` to shutdown.<br>
    * <br>
    * The default value is 60 seconds.
    */
  def closeTimeout: FiniteDuration

  /**
    * Creates a new [[ProducerSettings]] with the specified [[closeTimeout]].
    */
  def withCloseTimeout(closeTimeout: FiniteDuration): ProducerSettings

  /**
    * Includes the credentials properties from the provided [[KafkaCredentialStore]]
    */
  def withCredentials(credentialsStore: KafkaCredentialStore): ProducerSettings
}

object ProducerSettings {

  val default: ProducerSettings =
    ProducerSettingsImpl(
      customBlockingContext = None,
      properties = Map(
        ProducerConfig.RETRIES_CONFIG -> "0"
      ),
      closeTimeout = 60.seconds
    )

  private[kafka] case class ProducerSettingsImpl(
    override val customBlockingContext: Option[ExecutionContext],
    override val properties: Map[String, String],
    override val closeTimeout: FiniteDuration
  ) extends ProducerSettings {
    override def withCustomBlockingContext(ec: ExecutionContext): ProducerSettings =
      copy(customBlockingContext = Some(ec))

    override def withBootstrapServers(bootstrapServers: String): ProducerSettings =
      withProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

    override def withAcks(acks: Acks): ProducerSettings =
      withProperty(ProducerConfig.ACKS_CONFIG, acks match {
        case Acks.ZeroAcks => "0"
        case Acks.OneAcks  => "1"
        case Acks.AllAcks  => "all"
      })

    override def withBatchSize(batchSize: Int): ProducerSettings =
      withProperty(ProducerConfig.BATCH_SIZE_CONFIG, batchSize.toString)

    override def withClientId(clientId: String): ProducerSettings =
      withProperty(ProducerConfig.CLIENT_ID_CONFIG, clientId)

    override def withRetries(retries: Int): ProducerSettings =
      withProperty(ProducerConfig.RETRIES_CONFIG, retries.toString)

    override def withMaxInFlightRequestsPerConnection(
      maxInFlightRequestsPerConnection: Int
    ): ProducerSettings =
      withProperty(
        ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
        maxInFlightRequestsPerConnection.toString
      )

    override def withEnableIdempotence(enableIdempotence: Boolean): ProducerSettings =
      withProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotence.toString)

    override def withLinger(linger: FiniteDuration): ProducerSettings =
      withProperty(ProducerConfig.LINGER_MS_CONFIG, linger.toMillis.toString)

    override def withRequestTimeout(requestTimeout: FiniteDuration): ProducerSettings =
      withProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout.toMillis.toString)

    override def withDeliveryTimeout(deliveryTimeout: FiniteDuration): ProducerSettings =
      withProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeout.toMillis.toString)

    override def withProperty(key: String, value: String): ProducerSettings =
      copy(properties = properties.updated(key, value))

    override def withProperties(properties: (String, String)*): ProducerSettings =
      copy(properties = this.properties ++ properties.toMap)

    override def withProperties(properties: Map[String, String]): ProducerSettings =
      copy(properties = this.properties ++ properties)

    override def withCloseTimeout(closeTimeout: FiniteDuration): ProducerSettings =
      copy(closeTimeout = closeTimeout)

    /**
      * Includes the credentials properties from the provided [[KafkaCredentialStore]]
      */
    override def withCredentials(
      credentialsStore: KafkaCredentialStore
    ): ProducerSettings =
      withProperties(credentialsStore.properties)

    override def toString: String =
      s"ProducerSettings(closeTimeout = $closeTimeout)"
  }

  implicit val producerSettingsShow: Show[ProducerSettings] =
    Show.fromToString
}

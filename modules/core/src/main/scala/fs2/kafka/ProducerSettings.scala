/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.{Applicative, Show}
import fs2.kafka.security.KafkaCredentialStore
import org.apache.kafka.clients.producer.{ProducerConfig => JProducerConfig}

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
sealed abstract class ProducerSettings[F[_], K, V]
    extends SerializerSettings[F, K, V]
    with ProducerConfig[ProducerSettings[F, K, V]] {}

sealed trait SerializerSettings[F[_], K, V] {

  /**
    * The `Serializer` to use for serializing record keys.
    */
  def keySerializer: F[Serializer[F, K]]

  /**
    * The `Serializer` to use for serializing record values.
    */
  def valueSerializer: F[Serializer[F, V]]

}

sealed trait ProducerConfig[Settings <: ProducerConfig[Settings]] {
  self: Settings =>

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
    * JJProducerConfig.BOOTSTRAP_SERVERS_CONFIG
    * }}}
    */
  def withBootstrapServers(bootstrapServers: String): Settings

  /**
    * Returns a new [[ProducerSettings]] instance with the specified
    * acknowledgements. This is equivalent to setting the following
    * property using the [[withProperty]] function, except you can
    * specify it with [[Acks]] instead of a `String`.
    *
    * {{{
    * JJProducerConfig.ACKS_CONFIG
    * }}}
    */
  def withAcks(acks: Acks): Settings

  /**
    * Returns a new [[ProducerSettings]] instance with the specified
    * batch size. This is equivalent to setting the following property
    * using the [[withProperty]] function, except you can specify it
    * with an `Int` instead of a `String`.
    *
    * {{{
    * JJProducerConfig.BATCH_SIZE_CONFIG
    * }}}
    */
  def withBatchSize(batchSize: Int): Settings

  /**
    * Returns a new [[ProducerSettings]] instance with the specified
    * client id. This is equivalent to setting the following property
    * using the [[withProperty]] function.
    *
    * {{{
    * JJProducerConfig.CLIENT_ID_CONFIG
    * }}}
    */
  def withClientId(clientId: String): Settings

  /**
    * Returns a new [[ProducerSettings]] instance with the specified
    * retries. This is equivalent to setting the following property
    * using the [[withProperty]] function, except you can specify
    * it with an `Int` instead of a `String`.
    *
    * {{{
    * JJProducerConfig.RETRIES_CONFIG
    * }}}
    */
  def withRetries(retries: Int): Settings

  /**
    * Returns a new [[ProducerSettings]] instance with the specified
    * max in-flight requests per connection. This is equivalent to
    * setting the following property using the [[withProperty]]
    * function, except you can specify it with an `Int` instead
    * of a `String`.
    *
    * {{{
    * JJProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION
    * }}}
    */
  def withMaxInFlightRequestsPerConnection(
    maxInFlightRequestsPerConnection: Int
  ): Settings

  /**
    * Returns a new [[ProducerSettings]] instance with the specified
    * idempotence setting. This is equivalent to setting the following
    * property using the [[withProperty]] function, except you can
    * specify it with a `Boolean` instead of a `String`.
    *
    * {{{
    * JJProducerConfig.ENABLE_IDEMPOTENCE_CONFIG
    * }}}
    */
  def withEnableIdempotence(enableIdempotence: Boolean): Settings

  /**
    * Returns a new [[ProducerSettings]] instance with the specified
    * linger. This is equivalent to setting the following property
    * using the [[withProperty]] function, except you can specify
    * it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * JJProducerConfig.LINGER_MS_CONFIG
    * }}}
    */
  def withLinger(linger: FiniteDuration): Settings

  /**
    * Returns a new [[ProducerSettings]] instance with the specified
    * request timeout. This is equivalent to setting the following
    * property using the [[withProperty]] function, except you can
    * specify it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * JJProducerConfig.REQUEST_TIMEOUT_MS_CONFIG
    * }}}
    */
  def withRequestTimeout(requestTimeout: FiniteDuration): Settings

  /**
    * Returns a new [[ProducerSettings]] instance with the specified
    * delivery timeout. This is equivalent to setting the following
    * property using the [[withProperty]] function, except you can
    * specify it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * JJProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG
    * }}}
    */
  def withDeliveryTimeout(deliveryTimeout: FiniteDuration): Settings

  /**
    * Includes a property with the specified `key` and `value`.
    * The key should be one of the keys in `JProducerConfig`,
    * and the value should be a valid choice for the key.
    */
  def withProperty(key: String, value: String): Settings

  /**
    * Includes the specified keys and values as properties. The
    * keys should be part of the `JProducerConfig` keys, and
    * the values should be valid choices for the keys.
    */
  def withProperties(properties: (String, String)*): Settings

  /**
    * Includes the specified keys and values as properties. The
    * keys should be part of the `JProducerConfig` keys, and
    * the values should be valid choices for the keys.
    */
  def withProperties(properties: Map[String, String]): Settings

  /**
    * The time to wait for the Java `KafkaProducer` to shutdown.<br>
    * <br>
    * The default value is 60 seconds.
    */
  def closeTimeout: FiniteDuration

  /**
    * Creates a new [[ProducerSettings]] with the specified [[closeTimeout]].
    */
  def withCloseTimeout(closeTimeout: FiniteDuration): Settings

  /**
    * The maximum number of [[ProducerRecords]] to produce in the same batch.<br>
    * <br>
    * The default value is 10000.
    */
  def parallelism: Int

  /**
    * Creates a new [[ProducerSettings]] with the specified [[parallelism]].
    */
  def withParallelism(parallelism: Int): Settings

  /**
    * Includes the credentials properties from the provided [[KafkaCredentialStore]]
    */
  def withCredentials(credentialsStore: KafkaCredentialStore): Settings
}

object ProducerSettings {
  private[this] final case class ProducerSettingsImpl[F[_], K, V](
    override val keySerializer: F[Serializer[F, K]],
    override val valueSerializer: F[Serializer[F, V]],
    override val properties: Map[String, String],
    override val closeTimeout: FiniteDuration,
    override val parallelism: Int
  ) extends ProducerSettings[F, K, V] {

    override def withBootstrapServers(bootstrapServers: String): ProducerSettings[F, K, V] =
      withProperty(JProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

    override def withAcks(acks: Acks): ProducerSettings[F, K, V] =
      withProperty(JProducerConfig.ACKS_CONFIG, acks match {
        case Acks.ZeroAcks => "0"
        case Acks.OneAcks  => "1"
        case Acks.AllAcks  => "all"
      })

    override def withBatchSize(batchSize: Int): ProducerSettings[F, K, V] =
      withProperty(JProducerConfig.BATCH_SIZE_CONFIG, batchSize.toString)

    override def withClientId(clientId: String): ProducerSettings[F, K, V] =
      withProperty(JProducerConfig.CLIENT_ID_CONFIG, clientId)

    override def withRetries(retries: Int): ProducerSettings[F, K, V] =
      withProperty(JProducerConfig.RETRIES_CONFIG, retries.toString)

    override def withMaxInFlightRequestsPerConnection(
      maxInFlightRequestsPerConnection: Int
    ): ProducerSettings[F, K, V] =
      withProperty(
        JProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
        maxInFlightRequestsPerConnection.toString
      )

    override def withEnableIdempotence(enableIdempotence: Boolean): ProducerSettings[F, K, V] =
      withProperty(JProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotence.toString)

    override def withLinger(linger: FiniteDuration): ProducerSettings[F, K, V] =
      withProperty(JProducerConfig.LINGER_MS_CONFIG, linger.toMillis.toString)

    override def withRequestTimeout(requestTimeout: FiniteDuration): ProducerSettings[F, K, V] =
      withProperty(JProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout.toMillis.toString)

    override def withDeliveryTimeout(deliveryTimeout: FiniteDuration): ProducerSettings[F, K, V] =
      withProperty(JProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeout.toMillis.toString)

    override def withProperty(key: String, value: String): ProducerSettings[F, K, V] =
      copy(properties = properties.updated(key, value))

    override def withProperties(properties: (String, String)*): ProducerSettings[F, K, V] =
      copy(properties = this.properties ++ properties.toMap)

    override def withProperties(properties: Map[String, String]): ProducerSettings[F, K, V] =
      copy(properties = this.properties ++ properties)

    override def withCloseTimeout(closeTimeout: FiniteDuration): ProducerSettings[F, K, V] =
      copy(closeTimeout = closeTimeout)

    override def withParallelism(parallelism: Int): ProducerSettings[F, K, V] =
      copy(parallelism = parallelism)

    /**
      * Includes the credentials properties from the provided [[KafkaCredentialStore]]
      */
    override def withCredentials(
      credentialsStore: KafkaCredentialStore
    ): ProducerSettings[F, K, V] =
      withProperties(credentialsStore.properties)

    override def toString: String =
      s"ProducerSettings(closeTimeout = $closeTimeout)"
  }

  private[this] def create[F[_], K, V](
    keySerializer: F[Serializer[F, K]],
    valueSerializer: F[Serializer[F, V]]
  ): ProducerSettings[F, K, V] =
    ProducerSettingsImpl(
      keySerializer = keySerializer,
      valueSerializer = valueSerializer,
      properties = Map(
        JProducerConfig.RETRIES_CONFIG -> "0"
      ),
      closeTimeout = 60.seconds,
      parallelism = 10000
    )

  def apply[F[_], K, V](
    keySerializer: Serializer[F, K],
    valueSerializer: Serializer[F, V]
  )(implicit F: Applicative[F]): ProducerSettings[F, K, V] =
    create(
      keySerializer = F.pure(keySerializer),
      valueSerializer = F.pure(valueSerializer)
    )

  def apply[F[_], K, V](
    keySerializer: RecordSerializer[F, K],
    valueSerializer: Serializer[F, V]
  )(implicit F: Applicative[F]): ProducerSettings[F, K, V] =
    create(
      keySerializer = keySerializer.forKey,
      valueSerializer = F.pure(valueSerializer)
    )

  def apply[F[_], K, V](
    keySerializer: Serializer[F, K],
    valueSerializer: RecordSerializer[F, V]
  )(implicit F: Applicative[F]): ProducerSettings[F, K, V] =
    create(
      keySerializer = F.pure(keySerializer),
      valueSerializer = valueSerializer.forValue
    )

  def apply[F[_], K, V](
    implicit keySerializer: RecordSerializer[F, K],
    valueSerializer: RecordSerializer[F, V]
  ): ProducerSettings[F, K, V] =
    create(
      keySerializer = keySerializer.forKey,
      valueSerializer = valueSerializer.forValue
    )

  implicit def producerSettingsShow[F[_], K, V]: Show[ProducerSettings[F, K, V]] =
    Show.fromToString
}

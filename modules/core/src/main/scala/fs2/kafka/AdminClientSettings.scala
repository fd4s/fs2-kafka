/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.Sync
import cats.Show
import fs2.kafka.internal.converters.collection._
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import scala.concurrent.duration._

/**
  * [[AdminClientSettings]] contain settings necessary to create a
  * [[KafkaAdminClient]]. Several convenience functions are provided
  * so that you don't have to work with `String` values and keys from
  * `AdminClientConfig`. It's still possible to set `AdminClientConfig`
  * values with functions like [[withProperty]].<br>
  * <br>
  * [[AdminClientSettings]] instances are immutable and all modification
  * functions return a new [[AdminClientSettings]] instance.<br>
  * <br>
  * Use [[AdminClientSettings#apply]] for the default settings, and
  * then apply any desired modifications on top of that instance.
  */
sealed abstract class AdminClientSettings[F[_]] {

  /**
    * Properties which can be provided when creating a Java `KafkaAdminClient`
    * instance. Numerous functions in [[AdminClientSettings]] add properties
    * here if the settings are used by the Java `KafkaAdminClient`.
    */
  def properties: Map[String, String]

  /**
    * Returns a new [[AdminClientSettings]] instance with the specified
    * bootstrap servers. This is equivalent to setting the following
    * property using the [[withProperty]] function.
    *
    * {{{
    * AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG
    * }}}
    */
  def withBootstrapServers(bootstrapServers: String): AdminClientSettings[F]

  /**
    * Returns a new [[AdminClientSettings]] instance with the specified
    * client id. This is equivalent to setting the following property
    * using the [[withProperty]] function.
    *
    * {{{
    * AdminClientConfig.CLIENT_ID_CONFIG
    * }}}
    */
  def withClientId(clientId: String): AdminClientSettings[F]

  /**
    * Returns a new [[AdminClientSettings]] instance with the specified
    * reconnect backoff. This is equivalent to setting the following
    * property using the [[withProperty]] function, except you can
    * specify it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG
    * }}}
    */
  def withReconnectBackoff(reconnectBackoff: FiniteDuration): AdminClientSettings[F]

  /**
    * Returns a new [[AdminClientSettings]] instance with the specified
    * max reconnect backoff. This is equivalent to setting the following
    * property using the [[withProperty]] function, except you can
    * specify it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG
    * }}}
    */
  def withReconnectBackoffMax(reconnectBackoffMax: FiniteDuration): AdminClientSettings[F]

  /**
    * Returns a new [[AdminClientSettings]] instance with the specified
    * retry backoff. This is equivalent to setting the following property
    * using the [[withProperty]] function, except you can specify it with
    * a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * AdminClientConfig.RETRY_BACKOFF_MS_CONFIG
    * }}}
    */
  def withRetryBackoff(retryBackoff: FiniteDuration): AdminClientSettings[F]

  /**
    * Returns a new [[AdminClientSettings]] instance with the specified
    * max connection idle time. This is equivalent to setting the following
    * property using the [[withProperty]] function, except you can specify
    * it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG
    * }}}
    */
  def withConnectionsMaxIdle(connectionsMaxIdle: FiniteDuration): AdminClientSettings[F]

  /**
    * Returns a new [[AdminClientSettings]] instance with the specified
    * request timeout. This is equivalent to setting the following
    * property using the [[withProperty]] function, except you can
    * specify it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG
    * }}}
    */
  def withRequestTimeout(requestTimeout: FiniteDuration): AdminClientSettings[F]

  /**
    * Returns a new [[AdminClientSettings]] instance with the specified
    * max metadata age. This is equivalent to setting the following
    * property using the [[withProperty]] function, except you can
    * specify it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * AdminClientConfig.METADATA_MAX_AGE_CONFIG
    * }}}
    */
  def withMetadataMaxAge(metadataMaxAge: FiniteDuration): AdminClientSettings[F]

  /**
    * Returns a new [[AdminClientSettings]] instance with the specified
    * retries. This is equivalent to setting the following property
    * using the [[withProperty]] function, except you can specify
    * it with an `Int` instead of a `String`.
    *
    * {{{
    * AdminClientConfig.RETRIES_CONFIG
    * }}}
    */
  def withRetries(retries: Int): AdminClientSettings[F]

  /**
    * Includes a property with the specified `key` and `value`.
    * The key should be one of the keys in `AdminClientConfig`,
    * and the value should be a valid choice for the key.
    */
  def withProperty(key: String, value: String): AdminClientSettings[F]

  /**
    * Includes the specified keys and values as properties. The
    * keys should be part of the `AdminClientConfig` keys, and
    * the values should be valid choices for the keys.
    */
  def withProperties(properties: (String, String)*): AdminClientSettings[F]

  /**
    * Includes the specified keys and values as properties. The
    * keys should be part of the `AdminClientConfig` keys, and
    * the values should be valid choices for the keys.
    */
  def withProperties(properties: Map[String, String]): AdminClientSettings[F]

  /**
    * The time to wait for the Java `KafkaAdminClient` to shutdown.<br>
    * <br>
    * The default value is 20 seconds.
    */
  def closeTimeout: FiniteDuration

  /**
    * Creates a new [[AdminClientSettings]] with the specified [[closeTimeout]].
    */
  def withCloseTimeout(closeTimeout: FiniteDuration): AdminClientSettings[F]

  /**
    * Creates a new `KafkaAdminClient` using the [[properties]]. Note that
    * this operation should be bracketed, using e.g. `Resource`, to ensure
    * the `close` function on the admin client is called.
    */
  def createAdminClient: F[AdminClient]

  /**
    * Creates a new [[AdminClientSettings]] with the specified function for
    * creating `AdminClient` instances in [[createAdminClient]]. The argument
    * is the [[properties]] of the settings instance.
    */
  def withCreateAdminClient(
    createAdminClient: Map[String, String] => F[AdminClient]
  ): AdminClientSettings[F]
}

object AdminClientSettings {
  private[this] final case class AdminClientSettingsImpl[F[_]](
    override val properties: Map[String, String],
    override val closeTimeout: FiniteDuration,
    val createAdminClientWith: Map[String, String] => F[AdminClient]
  ) extends AdminClientSettings[F] {

    override def withBootstrapServers(bootstrapServers: String): AdminClientSettings[F] =
      withProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

    override def withClientId(clientId: String): AdminClientSettings[F] =
      withProperty(AdminClientConfig.CLIENT_ID_CONFIG, clientId)

    override def withReconnectBackoff(reconnectBackoff: FiniteDuration): AdminClientSettings[F] =
      withProperty(
        AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG,
        reconnectBackoff.toMillis.toString
      )

    override def withReconnectBackoffMax(
      reconnectBackoffMax: FiniteDuration
    ): AdminClientSettings[F] =
      withProperty(
        AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG,
        reconnectBackoffMax.toMillis.toString
      )

    override def withRetryBackoff(retryBackoff: FiniteDuration): AdminClientSettings[F] =
      withProperty(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, retryBackoff.toMillis.toString)

    override def withConnectionsMaxIdle(
      connectionsMaxIdle: FiniteDuration
    ): AdminClientSettings[F] =
      withProperty(
        AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG,
        connectionsMaxIdle.toMillis.toString
      )

    override def withRequestTimeout(requestTimeout: FiniteDuration): AdminClientSettings[F] =
      withProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout.toMillis.toString)

    override def withMetadataMaxAge(metadataMaxAge: FiniteDuration): AdminClientSettings[F] =
      withProperty(AdminClientConfig.METADATA_MAX_AGE_CONFIG, metadataMaxAge.toMillis.toString)

    override def withRetries(retries: Int): AdminClientSettings[F] =
      withProperty(AdminClientConfig.RETRIES_CONFIG, retries.toString)

    override def withProperty(key: String, value: String): AdminClientSettings[F] =
      copy(properties = properties.updated(key, value))

    override def withProperties(properties: (String, String)*): AdminClientSettings[F] =
      copy(properties = this.properties ++ properties.toMap)

    override def withProperties(properties: Map[String, String]): AdminClientSettings[F] =
      copy(properties = this.properties ++ properties)

    override def withCloseTimeout(closeTimeout: FiniteDuration): AdminClientSettings[F] =
      copy(closeTimeout = closeTimeout)

    override def createAdminClient: F[AdminClient] =
      createAdminClientWith(properties)

    override def withCreateAdminClient(
      createAdminClientWith: Map[String, String] => F[AdminClient]
    ): AdminClientSettings[F] =
      copy(createAdminClientWith = createAdminClientWith)

    override def toString: String =
      s"AdminClientSettings(closeTimeout = $closeTimeout)"
  }

  def apply[F[_]](implicit F: Sync[F]): AdminClientSettings[F] =
    AdminClientSettingsImpl(
      properties = Map.empty,
      closeTimeout = 20.seconds,
      createAdminClientWith = properties =>
        F.blocking {
          AdminClient.create {
            (properties: Map[String, AnyRef]).asJava
          }
        }
    )

  implicit def adminClientSettingsShow[F[_]]: Show[AdminClientSettings[F]] =
    Show.fromToString
}

/*
 * Copyright 2018-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.Show
import fs2.kafka.security.KafkaCredentialStore
import org.apache.kafka.clients.admin.AdminClientConfig
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
sealed abstract class AdminClientSettings extends KafkaClientSettings[AdminClientSettings] {

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
  def withReconnectBackoff(reconnectBackoff: FiniteDuration): AdminClientSettings

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
  def withReconnectBackoffMax(reconnectBackoffMax: FiniteDuration): AdminClientSettings

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
  def withRetryBackoff(retryBackoff: FiniteDuration): AdminClientSettings

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
  def withConnectionsMaxIdle(connectionsMaxIdle: FiniteDuration): AdminClientSettings

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
  def withMetadataMaxAge(metadataMaxAge: FiniteDuration): AdminClientSettings

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
  def withRetries(retries: Int): AdminClientSettings
}

object AdminClientSettings {
  private[this] final case class AdminClientSettingsImpl(
    override val properties: Map[String, String],
    override val closeTimeout: FiniteDuration
  ) extends AdminClientSettings {

    override def withReconnectBackoff(reconnectBackoff: FiniteDuration): AdminClientSettings =
      withProperty(
        AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG,
        reconnectBackoff.toMillis.toString
      )

    override def withReconnectBackoffMax(
      reconnectBackoffMax: FiniteDuration
    ): AdminClientSettings =
      withProperty(
        AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG,
        reconnectBackoffMax.toMillis.toString
      )

    override def withRetryBackoff(retryBackoff: FiniteDuration): AdminClientSettings =
      withProperty(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, retryBackoff.toMillis.toString)

    override def withConnectionsMaxIdle(
      connectionsMaxIdle: FiniteDuration
    ): AdminClientSettings =
      withProperty(
        AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG,
        connectionsMaxIdle.toMillis.toString
      )

    override def withMetadataMaxAge(metadataMaxAge: FiniteDuration): AdminClientSettings =
      withProperty(AdminClientConfig.METADATA_MAX_AGE_CONFIG, metadataMaxAge.toMillis.toString)

    override def withRetries(retries: Int): AdminClientSettings =
      withProperty(AdminClientConfig.RETRIES_CONFIG, retries.toString)

    override def withProperties(properties: Map[String, String]): AdminClientSettings =
      copy(properties = this.properties ++ properties)

    override def withCloseTimeout(closeTimeout: FiniteDuration): AdminClientSettings =
      copy(closeTimeout = closeTimeout)

    override def toString: String =
      s"AdminClientSettings(closeTimeout = $closeTimeout)"
  }

  @deprecated("use the overload that takes an argument for BootstrapServers", "2.0.0")
  def apply: AdminClientSettings =
    AdminClientSettingsImpl(
      properties = Map.empty,
      closeTimeout = 20.seconds
    )

  def apply(bootstrapServers: String): AdminClientSettings =
    AdminClientSettingsImpl(
      properties = Map.empty,
      closeTimeout = 20.seconds
    ).withBootstrapServers(bootstrapServers)

  implicit def adminClientSettingsShow[F[_]]: Show[AdminClientSettings] =
    Show.fromToString
}

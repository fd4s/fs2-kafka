/*
 * Copyright 2018-2022 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.schemaregistry.client

import cats.{Eq, Show}

/**
  * Describes how to create a `SchemaRegistryClient` and which
  * settings should be used. Settings are tailored for default
  * implementation `CachedSchemaRegistryClient`.
  *
  * Use `SchemaRegistryClient#apply` to create an instance.
  */
sealed trait SchemaRegistryClientSettings {

  /**
    * The base URL of the schema registry service.
    */
  def baseUrl: String

  /**
    * The maximum number of schemas to cache in the client.
    *
    * The default value is 1000.
    */
  def maxCacheSize: Int

  /**
    * Creates a new [[SchemaRegistryClientSettings]] instance
    * with the specified [[maxCacheSize]].
    */
  def withMaxCacheSize(maxCacheSize: Int): SchemaRegistryClientSettings

  /**
    * Creates a new [[SchemaRegistryClientSettings]] instance
    * with the specified authentication details.
    */
  def withAuth(auth: Auth): SchemaRegistryClientSettings

  /**
    * Properties provided when creating a `SchemaRegistryClient`.
    * Numerous functions in [[SchemaRegistryClientSettings]] add
    * properties here as necessary.
    */
  def properties: Map[String, String]

  /**
    * Creates a new [[SchemaRegistryClientSettings]] instance
    * including a property with the specified key and value.
    */
  def withProperty(key: String, value: String): SchemaRegistryClientSettings

  /**
    * Creates a new [[SchemaRegistryClientSettings]] instance
    * including properties with the specified keys and values.
    */
  def withProperties(properties: (String, String)*): SchemaRegistryClientSettings

  /**
    * Creates a new [[SchemaRegistryClientSettings]] instance
    * including properties with the specified keys and values.
    */
  def withProperties(properties: Map[String, String]): SchemaRegistryClientSettings
}

object SchemaRegistryClientSettings {

  private[this] final case class SchemaRegistryClientSettingsImpl(
    override val baseUrl: String,
    override val maxCacheSize: Int,
    override val properties: Map[String, String]
  ) extends SchemaRegistryClientSettings {

    override def withMaxCacheSize(maxCacheSize: Int): SchemaRegistryClientSettings =
      copy(maxCacheSize = maxCacheSize)

    override def withAuth(auth: Auth): SchemaRegistryClientSettings =
      auth match {
        case Auth.BasicAuth(username, password) =>
          withProperties(
            "basic.auth.credentials.source" -> "USER_INFO",
            "schema.registry.basic.auth.user.info" -> s"$username:$password"
          )

        case Auth.BearerAuth(token) =>
          withProperties(
            "bearer.auth.credentials.source" -> "STATIC_TOKEN",
            "bearer.auth.token" -> token
          )

        case Auth.NoAuth =>
          this
      }

    override def withProperty(key: String, value: String): SchemaRegistryClientSettings =
      copy(properties = properties.updated(key, value))

    override def withProperties(properties: (String, String)*): SchemaRegistryClientSettings =
      copy(properties = this.properties ++ properties.toMap)

    override def withProperties(properties: Map[String, String]): SchemaRegistryClientSettings =
      copy(properties = this.properties ++ properties)

    override def toString: String =
      s"SchemaRegistryClientSettings(baseUrl = $baseUrl, maxCacheSize = $maxCacheSize)"
  }

  /**
    * Creates a new [[SchemaRegistryClientSettings]] instance
    * using the specified base URL of the schema registry.
    */
  def apply(baseUrl: String): SchemaRegistryClientSettings =
    SchemaRegistryClientSettingsImpl(
      baseUrl = baseUrl,
      maxCacheSize = 1000,
      properties = Map.empty
    )

  implicit val schemaRegistryClientSettingsShow: Show[SchemaRegistryClientSettings] =
    Show.fromToString

  implicit val schemaRegistryClientSettingsEq: Eq[SchemaRegistryClientSettings] =
    Eq.fromUniversalEquals[SchemaRegistryClientSettingsImpl]
      .asInstanceOf[Eq[SchemaRegistryClientSettings]]
}

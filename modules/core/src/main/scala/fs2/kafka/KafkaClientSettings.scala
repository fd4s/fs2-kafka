package fs2.kafka

import fs2.kafka.security.KafkaCredentialStore
import org.apache.kafka.clients.CommonClientConfigs

import scala.concurrent.duration._

/**
  * Settings common to producers, consumers, and admin clients. Implemented by
  * [[ConsumerSettings]], [[ProducerSettings]], and [[AdminClientSettings]].
  */
private[kafka] trait KafkaClientSettings[Self <: KafkaClientSettings[Self]] { self =>

  /**
    * Properties which can be provided when creating a Java `KafkaProducer`
    * instance. Numerous functions in `Settings` classes add properties
    * here if the settings are used by the Java Kakfa client.
    */
  def properties: Map[String, String]

  /**
    * Includes a property with the specified `key` and `value`.
    * The key should be one of the config keys for the underlying,
    * Java client, and the value should be a valid choice for the key.
    */
  final def withProperty(key: String, value: String): Self =
    withProperties(properties.updated(key, value))

  /**
    * Includes the specified keys and values as properties.
    * The keys should be among the config keys for the underlying,
    * Java client, and the value should be a valid choice for the key.
    */
  final def withProperties(properties: (String, String)*): Self =
    withProperties(this.properties ++ properties.toMap)

  /**
    * Includes the specified keys and values as properties.
    * The keys should be among the config keys for the underlying,
    * Java client, and the value should be a valid choice for the key.
    */
  def withProperties(properties: Map[String, String]): Self

  /**
    * Returns a new `Settings` instance with the specified
    * bootstrap servers. This is equivalent to setting the following
    * property using the [[withProperty]] function.
    *
    * {{{
    * CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG
    * }}}
    */
  final def withBootstrapServers(bootstrapServers: String): Self =
    withProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

  /**
    * Returns a new `Settings` instance with the specified
    * client id. This is equivalent to setting the following property
    * using the [[withProperty]] function.
    *
    * {{{
    * ProducerConfig.CLIENT_ID_CONFIG
    * }}}
    */
  final def withClientId(clientId: String): Self =
    withProperty(CommonClientConfigs.CLIENT_ID_CONFIG, clientId)

  /**
    * Includes the credentials properties from the provided [[KafkaCredentialStore]]
    */
  final def withCredentials(credentialsStore: KafkaCredentialStore): Self =
    withProperties(credentialsStore.properties)

  /**
    * Returns a new `Settings` instance with the specified
    * request timeout. This is equivalent to setting the following
    * property using the [[withProperty]] function, except you can
    * specify it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG
    * }}}
    */
  final def withRequestTimeout(requestTimeout: FiniteDuration): Self =
    withProperty(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout.toMillis.toString)

  /**
    * The time to wait for the Java client to shutdown.<br>
    * <br>
    * The default value is 60 seconds.
    */
  def closeTimeout: FiniteDuration

  /**
    * Creates a new `Settings` instance with the specified [[closeTimeout]].
    */
  def withCloseTimeout(closeTimeout: FiniteDuration): Self
}

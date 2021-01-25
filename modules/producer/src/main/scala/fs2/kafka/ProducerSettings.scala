/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.{Blocker, Sync}
import cats.Show
import fs2.kafka.common._
import fs2.kafka.internal.converters.collection._
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArraySerializer
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
sealed abstract class ProducerSettings[F[_], K, V] {

  /**
    * The `Serializer` to use for serializing record keys.
    */
  def keySerializer: F[Serializer[F, K]]

  /**
    * The `Serializer` to use for serializing record values.
    */
  def valueSerializer: F[Serializer[F, V]]

  /**
    * The `Blocker` to use for blocking Kafka operations. If not
    * explicitly provided, a default `Blocker` will be created
    * when creating a `KafkaProducer` instance.
    */
  def blocker: Option[Blocker]

  /**
    * Returns a new [[ProducerSettings]] instance with the
    * specified [[blocker]] to use for blocking operations.
    */
  def withBlocker(blocker: Blocker): ProducerSettings[F, K, V]

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
  def withBootstrapServers(bootstrapServers: String): ProducerSettings[F, K, V]

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
  def withAcks(acks: Acks): ProducerSettings[F, K, V]

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
  def withBatchSize(batchSize: Int): ProducerSettings[F, K, V]

  /**
    * Returns a new [[ProducerSettings]] instance with the specified
    * client id. This is equivalent to setting the following property
    * using the [[withProperty]] function.
    *
    * {{{
    * ProducerConfig.CLIENT_ID_CONFIG
    * }}}
    */
  def withClientId(clientId: String): ProducerSettings[F, K, V]

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
  def withRetries(retries: Int): ProducerSettings[F, K, V]

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
  ): ProducerSettings[F, K, V]

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
  def withEnableIdempotence(enableIdempotence: Boolean): ProducerSettings[F, K, V]

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
  def withLinger(linger: FiniteDuration): ProducerSettings[F, K, V]

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
  def withRequestTimeout(requestTimeout: FiniteDuration): ProducerSettings[F, K, V]

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
  def withDeliveryTimeout(deliveryTimeout: FiniteDuration): ProducerSettings[F, K, V]

  /**
    * Includes a property with the specified `key` and `value`.
    * The key should be one of the keys in `ProducerConfig`,
    * and the value should be a valid choice for the key.
    */
  def withProperty(key: String, value: String): ProducerSettings[F, K, V]

  /**
    * Includes the specified keys and values as properties. The
    * keys should be part of the `ProducerConfig` keys, and
    * the values should be valid choices for the keys.
    */
  def withProperties(properties: (String, String)*): ProducerSettings[F, K, V]

  /**
    * Includes the specified keys and values as properties. The
    * keys should be part of the `ProducerConfig` keys, and
    * the values should be valid choices for the keys.
    */
  def withProperties(properties: Map[String, String]): ProducerSettings[F, K, V]

  /**
    * The time to wait for the Java `KafkaProducer` to shutdown.<br>
    * <br>
    * The default value is 60 seconds.
    */
  def closeTimeout: FiniteDuration

  /**
    * Creates a new [[ProducerSettings]] with the specified [[closeTimeout]].
    */
  def withCloseTimeout(closeTimeout: FiniteDuration): ProducerSettings[F, K, V]

  /**
    * The maximum number of [[ProducerRecords]] to produce in the same batch.<br>
    * <br>
    * The default value is 10000.
    */
  def parallelism: Int

  /**
    * Creates a new [[ProducerSettings]] with the specified [[parallelism]].
    */
  def withParallelism(parallelism: Int): ProducerSettings[F, K, V]

  /**
    * Creates a new `Producer` using the [[properties]]. Note that this
    * operation should be bracketed, using e.g. `Resource`, to ensure
    * the `close` function on the producer is called.
    */
  def createProducer: F[JavaByteProducer]

  /**
    * Creates a new [[ProducerSettings]] with the specified function for
    * creating `Producer` instances in [[createProducer]]. The argument
    * is the [[properties]] of the settings instance.
    */
  def withCreateProducer(
    createProducer: Map[String, String] => F[
      org.apache.kafka.clients.producer.Producer[Array[Byte], Array[Byte]]
    ]
  ): ProducerSettings[F, K, V]
}

object ProducerSettings {
  private[this] final case class ProducerSettingsImpl[F[_], K, V](
    override val keySerializer: F[Serializer[F, K]],
    override val valueSerializer: F[Serializer[F, V]],
    override val blocker: Option[Blocker],
    override val properties: Map[String, String],
    override val closeTimeout: FiniteDuration,
    override val parallelism: Int,
    val createProducerWith: Map[String, String] => F[
      org.apache.kafka.clients.producer.Producer[Array[Byte], Array[Byte]]
    ]
  ) extends ProducerSettings[F, K, V] {
    override def withBlocker(blocker: Blocker): ProducerSettings[F, K, V] =
      copy(blocker = Some(blocker))

    override def withBootstrapServers(bootstrapServers: String): ProducerSettings[F, K, V] =
      withProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

    override def withAcks(acks: Acks): ProducerSettings[F, K, V] =
      withProperty(ProducerConfig.ACKS_CONFIG, acks match {
        case Acks.ZeroAcks => "0"
        case Acks.OneAcks  => "1"
        case Acks.AllAcks  => "all"
      })

    override def withBatchSize(batchSize: Int): ProducerSettings[F, K, V] =
      withProperty(ProducerConfig.BATCH_SIZE_CONFIG, batchSize.toString)

    override def withClientId(clientId: String): ProducerSettings[F, K, V] =
      withProperty(ProducerConfig.CLIENT_ID_CONFIG, clientId)

    override def withRetries(retries: Int): ProducerSettings[F, K, V] =
      withProperty(ProducerConfig.RETRIES_CONFIG, retries.toString)

    override def withMaxInFlightRequestsPerConnection(
      maxInFlightRequestsPerConnection: Int
    ): ProducerSettings[F, K, V] =
      withProperty(
        ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
        maxInFlightRequestsPerConnection.toString
      )

    override def withEnableIdempotence(enableIdempotence: Boolean): ProducerSettings[F, K, V] =
      withProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotence.toString)

    override def withLinger(linger: FiniteDuration): ProducerSettings[F, K, V] =
      withProperty(ProducerConfig.LINGER_MS_CONFIG, linger.toMillis.toString)

    override def withRequestTimeout(requestTimeout: FiniteDuration): ProducerSettings[F, K, V] =
      withProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout.toMillis.toString)

    override def withDeliveryTimeout(deliveryTimeout: FiniteDuration): ProducerSettings[F, K, V] =
      withProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeout.toMillis.toString)

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

    override def createProducer: F[JavaByteProducer] =
      createProducerWith(properties)

    override def withCreateProducer(
      createProducerWith: Map[String, String] => F[
        org.apache.kafka.clients.producer.Producer[Array[Byte], Array[Byte]]
      ]
    ): ProducerSettings[F, K, V] =
      copy(createProducerWith = createProducerWith)

    override def toString: String =
      s"ProducerSettings(closeTimeout = $closeTimeout)"
  }

  private[this] def create[F[_], K, V](
    keySerializer: F[Serializer[F, K]],
    valueSerializer: F[Serializer[F, V]]
  )(implicit F: Sync[F]): ProducerSettings[F, K, V] =
    ProducerSettingsImpl(
      keySerializer = keySerializer,
      valueSerializer = valueSerializer,
      blocker = None,
      properties = Map(
        ProducerConfig.RETRIES_CONFIG -> "0"
      ),
      closeTimeout = 60.seconds,
      parallelism = 10000,
      createProducerWith = properties =>
        F.delay {
          val byteArraySerializer = new ByteArraySerializer
          new org.apache.kafka.clients.producer.KafkaProducer(
            (properties: Map[String, AnyRef]).asJava,
            byteArraySerializer,
            byteArraySerializer
          )
        }
    )

  def apply[F[_], K, V](
    keySerializer: Serializer[F, K],
    valueSerializer: Serializer[F, V]
  )(implicit F: Sync[F]): ProducerSettings[F, K, V] =
    create(
      keySerializer = F.pure(keySerializer),
      valueSerializer = F.pure(valueSerializer)
    )

  def apply[F[_], K, V](
    keySerializer: RecordSerializer[F, K],
    valueSerializer: Serializer[F, V]
  )(implicit F: Sync[F]): ProducerSettings[F, K, V] =
    create(
      keySerializer = keySerializer.forKey,
      valueSerializer = F.pure(valueSerializer)
    )

  def apply[F[_], K, V](
    keySerializer: Serializer[F, K],
    valueSerializer: RecordSerializer[F, V]
  )(implicit F: Sync[F]): ProducerSettings[F, K, V] =
    create(
      keySerializer = F.pure(keySerializer),
      valueSerializer = valueSerializer.forValue
    )

  def apply[F[_], K, V](
    keySerializer: RecordSerializer[F, K],
    valueSerializer: RecordSerializer[F, V]
  )(implicit F: Sync[F]): ProducerSettings[F, K, V] =
    create(
      keySerializer = keySerializer.forKey,
      valueSerializer = valueSerializer.forValue
    )

  def apply[F[_], K, V](
    implicit F: Sync[F],
    keySerializer: RecordSerializer[F, K],
    valueSerializer: RecordSerializer[F, V]
  ): ProducerSettings[F, K, V] =
    create(
      keySerializer = keySerializer.forKey,
      valueSerializer = valueSerializer.forValue
    )

  implicit def producerSettingsShow[F[_], K, V]: Show[ProducerSettings[F, K, V]] =
    Show.fromToString
}

/*
 * Copyright 2018-2019 OVO Energy Limited
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
import cats.Show
import org.apache.kafka.clients.producer.{Producer, ProducerConfig}
import org.apache.kafka.common.serialization.ByteArraySerializer
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

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
  def keySerializer: Serializer[F, K]

  /**
    * The `Serializer` to use for serializing record values.
    */
  def valueSerializer: Serializer[F, V]

  /**
    * The `ExecutionContext` on which to run blocking Kafka operations.
    * If not explicitly provided, a default `ExecutionContext` will be
    * instantiated when creating a `KafkaProducer` instance.
    */
  def executionContext: Option[ExecutionContext]

  /**
    * Returns a new [[ProducerSettings]] instance with the specified
    * [[executionContext]] on which to run blocking Kafka operations.
    */
  def withExecutionContext(
    executionContext: ExecutionContext
  ): ProducerSettings[F, K, V]

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
    * Returns a new [[ProducerSettings]] instance with the specified
    * transactional ID. This is equivalent to setting the following
    * property using the [[withProperty]] function.
    *
    * {{{
    * ProducerConfig.TRANSACTIONAL_ID_CONFIG
    * }}}
    */
  def withTransactionalId(transactionalId: String): ProducerSettings[F, K, V]

  /**
    * Returns a new [[ProducerSettings]] instance with the specified
    * transactional ID. This is equivalent to setting the following
    * property using the [[withProperty]] function, except you can specify
    * it with a `FiniteDuration` instead of a `String`
    *
    * {{{
    * ProducerConfig.TRANSACTION_TIMEOUT_CONFIG
    * }}}
    */
  def withTransactionTimeout(transactionTimeout: FiniteDuration): ProducerSettings[F, K, V]

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
    * Whether serialization should be run on [[executionContext]] or not.
    * When `true`, we will shift to run on the [[executionContext]] for
    * the duration of serialization. When `false`, no such shifting
    * will take place.<br>
    * <br>
    * Serialization is shifted to [[executionContext]] by default, in
    * order to support blocking serializers. If your serializers aren't
    * blocking, then this can safely be set to `false`.<br>
    * <br>
    * The default value is `true`.
    */
  def shiftSerialization: Boolean

  /**
    * Creates a new [[ProducerSettings]] with the specified [[shiftSerialization]].
    */
  def withShiftSerialization(shiftSerialization: Boolean): ProducerSettings[F, K, V]

  /**
    * Creates a new `Producer` using the [[properties]]. Note that this
    * operation should be bracketed, using e.g. `Resource`, to ensure
    * the `close` function on the producer is called.
    */
  def createProducer: F[Producer[Array[Byte], Array[Byte]]]

  /**
    * Creates a new [[ProducerSettings]] with the specified function for
    * creating `Producer` instances in [[createProducer]]. The argument
    * is the [[properties]] of the settings instance.
    */
  def withCreateProducer(
    createProducer: Map[String, String] => F[Producer[Array[Byte], Array[Byte]]]
  ): ProducerSettings[F, K, V]
}

object ProducerSettings {
  private[this] final case class ProducerSettingsImpl[F[_], K, V](
    override val keySerializer: Serializer[F, K],
    override val valueSerializer: Serializer[F, V],
    override val executionContext: Option[ExecutionContext],
    override val properties: Map[String, String],
    override val closeTimeout: FiniteDuration,
    override val shiftSerialization: Boolean,
    val createProducerWith: Map[String, String] => F[Producer[Array[Byte], Array[Byte]]]
  ) extends ProducerSettings[F, K, V] {
    override def withExecutionContext(
      executionContext: ExecutionContext
    ): ProducerSettings[F, K, V] =
      copy(executionContext = Some(executionContext))

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

    override def withTransactionalId(transactionalId: String): ProducerSettings[F, K, V] =
      withProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId)

    override def withTransactionTimeout(
      transactionTimeout: FiniteDuration): ProducerSettings[F, K, V] =
      withProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeout.toMillis.toString)

    override def withProperty(key: String, value: String): ProducerSettings[F, K, V] =
      copy(properties = properties.updated(key, value))

    override def withProperties(properties: (String, String)*): ProducerSettings[F, K, V] =
      copy(properties = this.properties ++ properties.toMap)

    override def withProperties(properties: Map[String, String]): ProducerSettings[F, K, V] =
      copy(properties = this.properties ++ properties)

    override def withCloseTimeout(closeTimeout: FiniteDuration): ProducerSettings[F, K, V] =
      copy(closeTimeout = closeTimeout)

    override def withShiftSerialization(shiftSerialization: Boolean): ProducerSettings[F, K, V] =
      copy(shiftSerialization = shiftSerialization)

    override def createProducer: F[Producer[Array[Byte], Array[Byte]]] =
      createProducerWith(properties)

    override def withCreateProducer(
      createProducerWith: Map[String, String] => F[Producer[Array[Byte], Array[Byte]]]
    ): ProducerSettings[F, K, V] =
      copy(createProducerWith = createProducerWith)

    override def toString: String =
      s"ProducerSettings(closeTimeout = $closeTimeout, shiftSerialization = $shiftSerialization)"
  }

  private[this] def create[F[_], K, V](
    keySerializer: Serializer[F, K],
    valueSerializer: Serializer[F, V]
  )(implicit F: Sync[F]): ProducerSettings[F, K, V] =
    ProducerSettingsImpl(
      keySerializer = keySerializer,
      valueSerializer = valueSerializer,
      executionContext = None,
      properties = Map.empty,
      closeTimeout = 60.seconds,
      shiftSerialization = true,
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

  /**
    * Creates a new [[ProducerSettings]] instance using the
    * specified serializers for the key and value.<br>
    * <br>
    * Since some Kafka operations are blocking, these should
    * be run on a dedicated `ExecutionContext`. When not set
    * with [[ProducerSettings#withExecutionContext]], there
    * will be a default created. The default context is the
    * same as `producerExecutionContextResource`.
    */
  def apply[F[_], K, V](
    keySerializer: Serializer[F, K],
    valueSerializer: Serializer[F, V]
  )(implicit F: Sync[F]): ProducerSettings[F, K, V] =
    create(keySerializer, valueSerializer)

  /**
    * Creates a new [[ProducerSettings]] instance using
    * implicit [[Serializer]]s for the key and value.
    */
  def apply[F[_], K, V](
    implicit F: Sync[F],
    keySerializer: Serializer[F, K],
    valueSerializer: Serializer[F, V]
  ): ProducerSettings[F, K, V] =
    create(keySerializer, valueSerializer)

  implicit def producerSettingsShow[F[_], K, V]: Show[ProducerSettings[F, K, V]] =
    Show.fromToString
}

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

import cats.Show
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.requests.OffsetFetchResponse
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

/**
  * [[ConsumerSettings]] contain settings necessary to create a
  * [[KafkaConsumer]]. At the very least, this includes key and
  * value deserializers.<br>
  * <br>
  * Several convenience functions are provided so that you don't
  * have to work with `String` values and `ConsumerConfig` for
  * configuration. It's still possible to specify `ConsumerConfig`
  * values with functions like [[withProperty]].<br>
  * <br>
  * [[ConsumerSettings]] instances are immutable and all modification
  * functions return a new [[ConsumerSettings]] instance.<br>
  * <br>
  * Use `ConsumerSettings#apply` to create a new instance.
  */
sealed abstract class ConsumerSettings[K, V] {

  /**
    * The `Deserializer` to use for deserializing record keys.
    */
  def keyDeserializer: KafkaDeserializer[K]

  /**
    * The `Deserializer` to use for deserializing record values.
    */
  def valueDeserializer: KafkaDeserializer[V]

  /**
    * The `ExecutionContext` on which to run blocking Kafka operations.
    * If not explicitly provided, a default `ExecutionContext` will be
    * instantiated when creating a `KafkaConsumer` instance.
    */
  def executionContext: Option[ExecutionContext]

  /**
    * Properties which can be provided when creating a Java `KafkaConsumer`
    * instance. Numerous functions in [[ConsumerSettings]] add properties
    * here if the settings are used by the Java `KafkaConsumer`.
    */
  def properties: Map[String, String]

  /**
    * Returns a new [[ConsumerSettings]] instance with the specified
    * bootstrap servers. This is equivalent to setting the following
    * property using the [[withProperty]] function.
    *
    * {{{
    * ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG
    * }}}
    */
  def withBootstrapServers(bootstrapServers: String): ConsumerSettings[K, V]

  /**
    * Returns a new [[ConsumerSettings]] instance with the specified
    * auto offset reset. This is equivalent to setting the following
    * property using the [[withProperty]] function, except you can
    * specify it with [[AutoOffsetReset]] instead of a `String`.
    *
    * {{{
    * ConsumerConfig.AUTO_OFFSET_RESET_CONFIG
    * }}}
    */
  def withAutoOffsetReset(autoOffsetReset: AutoOffsetReset): ConsumerSettings[K, V]

  /**
    * Returns a new [[ConsumerSettings]] instance with the specified
    * client id. This is equivalent to setting the following property
    * using the [[withProperty]] function.
    *
    * {{{
    * ConsumerConfig.CLIENT_ID_CONFIG
    * }}}
    */
  def withClientId(clientId: String): ConsumerSettings[K, V]

  /**
    * Returns a new [[ConsumerSettings]] instance with the specified
    * group id. This is equivalent to setting the following property
    * using the [[withProperty]] function.
    *
    * {{{
    * ConsumerConfig.GROUP_ID_CONFIG
    * }}}
    */
  def withGroupId(groupId: String): ConsumerSettings[K, V]

  /**
    * Returns a new [[ConsumerSettings]] instance with the specified
    * max poll records. This is equivalent to setting the following
    * property using the [[withProperty]] function, except you can
    * specify it with an `Int` instead of a `String`.
    *
    * {{{
    * ConsumerConfig.MAX_POLL_RECORDS_CONFIG
    * }}}
    */
  def withMaxPollRecords(maxPollRecords: Int): ConsumerSettings[K, V]

  /**
    * Returns a new [[ConsumerSettings]] instance with the specified
    * max poll interval. This is equivalent to setting the following
    * property using the [[withProperty]] function, except you can
    * specify it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG
    * }}}
    */
  def withMaxPollInterval(maxPollInterval: FiniteDuration): ConsumerSettings[K, V]

  /**
    * Returns a new [[ConsumerSettings]] instance with the specified
    * session timeout. This is equivalent to setting the following
    * property using the [[withProperty]] function, except you can
    * specify it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG
    * }}}
    */
  def withSessionTimeout(sessionTimeout: FiniteDuration): ConsumerSettings[K, V]

  /**
    * Returns a new [[ConsumerSettings]] instance with the specified
    * heartbeat interval. This is equivalent to setting the following
    * property using the [[withProperty]] function, except you can
    * specify it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG
    * }}}
    */
  def withHeartbeatInterval(heartbeatInterval: FiniteDuration): ConsumerSettings[K, V]

  /**
    * Returns a new [[ConsumerSettings]] instance with the specified
    * auto commit setting. This is equivalent to setting the following
    * property using the [[withProperty]] function, except you can
    * specify it with a `Boolean` instead of a `String`.
    *
    * {{{
    * ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG
    * }}}
    *
    * Note that by default, this setting is set to `false`.
    */
  def withEnableAutoCommit(enableAutoCommit: Boolean): ConsumerSettings[K, V]

  /**
    * Returns a new [[ConsumerSettings]] instance with the specified
    * auto commit interval. This is equivalent to setting the following
    * property using the [[withProperty]] function, except you can
    * specify it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG
    * }}}
    */
  def withAutoCommitInterval(autoCommitInterval: FiniteDuration): ConsumerSettings[K, V]

  /**
    * Returns a new [[ConsumerSettings]] instance with the specified
    * request timeout. This is equivalent to setting the following
    * property using the [[withProperty]] function, except you can
    * specify it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG
    * }}}
    */
  def withRequestTimeout(requestTimeout: FiniteDuration): ConsumerSettings[K, V]

  /**
    * Returns a new [[ConsumerSettings]] instance with the specified
    * default api timeout. This is equivalent to setting the following
    * property using the [[withProperty]] function, except you can
    * specify it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG
    * }}}
    */
  def withDefaultApiTimeout(defaultApiTimeout: FiniteDuration): ConsumerSettings[K, V]

  /**
    * Includes a property with the specified `key` and `value`.
    * The key should be one of the keys in `ConsumerConfig`,
    * and the value should be a valid choice for the key.
    */
  def withProperty(key: String, value: String): ConsumerSettings[K, V]

  /**
    * Includes the specified keys and values as properties. The
    * keys should be part of the `ConsumerConfig` keys, and
    * the values should be valid choices for the keys.
    */
  def withProperties(properties: (String, String)*): ConsumerSettings[K, V]

  /**
    * Includes the specified keys and values as properties. The
    * keys should be part of the `ConsumerConfig` keys, and
    * the values should be valid choices for the keys.
    */
  def withProperties(properties: Map[String, String]): ConsumerSettings[K, V]

  /**
    * The time to wait for the Java `KafkaConsumer` to shutdown.<br>
    * <br>
    * The default value is 20 seconds.
    */
  def closeTimeout: FiniteDuration

  /**
    * Creates a new [[ConsumerSettings]] with the specified [[closeTimeout]].
    */
  def withCloseTimeout(closeTimeout: FiniteDuration): ConsumerSettings[K, V]

  /**
    * The time to wait for offset commits to complete. If an offset commit
    * doesn't complete within this time, a [[CommitTimeoutException]] will
    * be raised instead.<br>
    * <br>
    * The default value is 15 seconds.
    */
  def commitTimeout: FiniteDuration

  /**
    * Creates a new [[ConsumerSettings]] with the specified [[commitTimeout]].
    */
  def withCommitTimeout(commitTimeout: FiniteDuration): ConsumerSettings[K, V]

  /**
    * How often we should attempt to call `poll` on the Java `KafkaConsumer`.<br>
    * <br>
    * The default value is 50 milliseconds.
    */
  def pollInterval: FiniteDuration

  /**
    * Creates a new [[ConsumerSettings]] with the specified [[pollInterval]].
    */
  def withPollInterval(pollInterval: FiniteDuration): ConsumerSettings[K, V]

  /**
    * How long we should allow the `poll` call to block for in the
    * Java `KafkaConsumer`.<br>
    * <br>
    * The default value is 50 milliseconds.
    */
  def pollTimeout: FiniteDuration

  /**
    * Creates a new [[ConsumerSettings]] with the specified [[pollTimeout]].
    */
  def withPollTimeout(pollTimeout: FiniteDuration): ConsumerSettings[K, V]

  /**
    * The [[CommitRecovery]] strategy for recovering from offset
    * commit exceptions.<br>
    * <br>
    * The default is [[CommitRecovery#Default]].
    */
  def commitRecovery: CommitRecovery

  /**
    * Creates a new [[ConsumerSettings]] with the specified
    * [[CommitRecovery]] as the [[commitRecovery]] to use.
    */
  def withCommitRecovery(commitRecovery: CommitRecovery): ConsumerSettings[K, V]

  /**
    * The [[ConsumerFactory]] for creating the Java `Consumer`.
    * The default is [[ConsumerFactory#Default]]. Note that you
    * normally don't need to have a custom [[ConsumerFactory]],
    * and you should instead prefer to create a custom trait or
    * class similar to [[KafkaConsumer]] for testing purposes.
    */
  def consumerFactory: ConsumerFactory

  /**
    * Creates a new [[ConsumerSettings]] with the specified
    * [[ConsumerFactory]] as the [[consumerFactory]] to use.
    * Note that under normal usage you don't need to have a
    * custom [[ConsumerFactory]] instance. For testing, you
    * should prefer to use a custom trait or class similar
    * to [[KafkaConsumer]].
    */
  def withConsumerFactory(consumerFactory: ConsumerFactory): ConsumerSettings[K, V]

  /**
    * The function used to specify metadata for records. This
    * metadata will be included in `OffsetAndMetadata` in the
    * [[CommittableOffset]]s, and can then be committed with
    * the offsets.<br>
    * <br>
    * By default, there will be no metadata, as determined by
    * `OffsetFetchResponse.NO_METADATA`.
    */
  def recordMetadata: ConsumerRecord[K, V] => String

  /**
    * Creates a new [[ConsumerSettings]] with the specified [[recordMetadata]].
    */
  def withRecordMetadata(recordMetadata: ConsumerRecord[K, V] => String): ConsumerSettings[K, V]

  /**
    * The maximum number of record batches to prefetch per topic-partition.
    * This means that, while records are being processed, there can be up
    * to `maxPrefetchBatches * max.poll.records` records per topic-partition
    * that have already been fetched, and are waiting to be processed. You can
    * use [[withMaxPollRecords]] to control the `max.poll.records` setting.<br>
    * <br>
    * This setting effectively controls backpressure, i.e. the maximum number
    * of batches to prefetch per topic-parititon before starting to slow down
    * (not fetching more records) until processing has caught-up.<br>
    * <br>
    * Note that prefetching cannot be disabled and is generally preferred since
    * it yields improved performance. The minimum value for this setting is `2`.
    */
  def maxPrefetchBatches: Int

  /**
    * Creates a new [[ConsumerSettings]] with the specified value
    * for [[maxPrefetchBatches]]. Note that if a value lower than
    * the minimum `2` is specified, [[maxPrefetchBatches]] will
    * instead be set to `2` and not the specified value.
    */
  def withMaxPrefetchBatches(maxPrefetchBatches: Int): ConsumerSettings[K, V]
}

object ConsumerSettings {
  private[this] final case class ConsumerSettingsImpl[K, V](
    override val keyDeserializer: KafkaDeserializer[K],
    override val valueDeserializer: KafkaDeserializer[V],
    override val executionContext: Option[ExecutionContext],
    override val properties: Map[String, String],
    override val closeTimeout: FiniteDuration,
    override val commitTimeout: FiniteDuration,
    override val pollInterval: FiniteDuration,
    override val pollTimeout: FiniteDuration,
    override val commitRecovery: CommitRecovery,
    override val consumerFactory: ConsumerFactory,
    override val recordMetadata: ConsumerRecord[K, V] => String,
    override val maxPrefetchBatches: Int
  ) extends ConsumerSettings[K, V] {
    override def withBootstrapServers(bootstrapServers: String): ConsumerSettings[K, V] =
      withProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

    override def withAutoOffsetReset(autoOffsetReset: AutoOffsetReset): ConsumerSettings[K, V] =
      withProperty(
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        autoOffsetReset match {
          case AutoOffsetReset.EarliestOffsetReset => "earliest"
          case AutoOffsetReset.LatestOffsetReset   => "latest"
          case AutoOffsetReset.NoOffsetReset       => "none"
        }
      )

    override def withClientId(clientId: String): ConsumerSettings[K, V] =
      withProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientId)

    override def withGroupId(groupId: String): ConsumerSettings[K, V] =
      withProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)

    override def withMaxPollRecords(maxPollRecords: Int): ConsumerSettings[K, V] =
      withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords.toString)

    override def withMaxPollInterval(maxPollInterval: FiniteDuration): ConsumerSettings[K, V] =
      withProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollInterval.toMillis.toString)

    override def withSessionTimeout(sessionTimeout: FiniteDuration): ConsumerSettings[K, V] =
      withProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeout.toMillis.toString)

    override def withHeartbeatInterval(heartbeatInterval: FiniteDuration): ConsumerSettings[K, V] =
      withProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatInterval.toMillis.toString)

    override def withEnableAutoCommit(enableAutoCommit: Boolean): ConsumerSettings[K, V] =
      withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit.toString)

    override def withAutoCommitInterval(
      autoCommitInterval: FiniteDuration
    ): ConsumerSettings[K, V] =
      withProperty(
        ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
        autoCommitInterval.toMillis.toString
      )

    override def withRequestTimeout(requestTimeout: FiniteDuration): ConsumerSettings[K, V] =
      withProperty(
        ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG,
        requestTimeout.toMillis.toString
      )

    override def withDefaultApiTimeout(defaultApiTimeout: FiniteDuration): ConsumerSettings[K, V] =
      withProperty(
        ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG,
        defaultApiTimeout.toMillis.toString
      )

    override def withProperty(key: String, value: String): ConsumerSettings[K, V] =
      copy(properties = properties.updated(key, value))

    override def withProperties(properties: (String, String)*): ConsumerSettings[K, V] =
      copy(properties = this.properties ++ properties.toMap)

    override def withProperties(properties: Map[String, String]): ConsumerSettings[K, V] =
      copy(properties = this.properties ++ properties)

    override def withCloseTimeout(closeTimeout: FiniteDuration): ConsumerSettings[K, V] =
      copy(closeTimeout = closeTimeout)

    override def withCommitTimeout(commitTimeout: FiniteDuration): ConsumerSettings[K, V] =
      copy(commitTimeout = commitTimeout)

    override def withPollInterval(pollInterval: FiniteDuration): ConsumerSettings[K, V] =
      copy(pollInterval = pollInterval)

    override def withPollTimeout(pollTimeout: FiniteDuration): ConsumerSettings[K, V] =
      copy(pollTimeout = pollTimeout)

    override def withCommitRecovery(commitRecovery: CommitRecovery): ConsumerSettings[K, V] =
      copy(commitRecovery = commitRecovery)

    override def withConsumerFactory(consumerFactory: ConsumerFactory): ConsumerSettings[K, V] =
      copy(consumerFactory = consumerFactory)

    override def withRecordMetadata(
      recordMetadata: ConsumerRecord[K, V] => String
    ): ConsumerSettings[K, V] =
      copy(recordMetadata = recordMetadata)

    override def withMaxPrefetchBatches(maxPrefetchBatches: Int): ConsumerSettings[K, V] =
      copy(maxPrefetchBatches = Math.max(2, maxPrefetchBatches))

    override def toString: String =
      Show[ConsumerSettings[K, V]].show(this)
  }

  private[this] def create[K, V](
    keyDeserializer: KafkaDeserializer[K],
    valueDeserializer: KafkaDeserializer[V],
    executionContext: Option[ExecutionContext]
  ): ConsumerSettings[K, V] = ConsumerSettingsImpl(
    keyDeserializer = keyDeserializer,
    valueDeserializer = valueDeserializer,
    executionContext = executionContext,
    properties = Map(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"),
    closeTimeout = 20.seconds,
    commitTimeout = 15.seconds,
    pollInterval = 50.millis,
    pollTimeout = 50.millis,
    commitRecovery = CommitRecovery.Default,
    consumerFactory = ConsumerFactory.Default,
    recordMetadata = _ => OffsetFetchResponse.NO_METADATA,
    maxPrefetchBatches = 2
  )

  /**
    * Creates a new [[ConsumerSettings]] instance using the
    * specified settings. Since offset commits are managed
    * manually, automatic commits are disabled by default.
    * Automatic offset commits can be enabled again using
    * [[ConsumerSettings#withEnableAutoCommit]].<br>
    * <br>
    * Since some Kafka operations are blocking, these should
    * be run on a dedicated `ExecutionContext`. When no such
    * `ExecutionContext` is specified, a default one will be
    * used. The default `ExecutionContext` is equivalent to
    * a `consumerExecutionContextResource` with `1` thread.
    */
  def apply[K, V](
    keyDeserializer: KafkaDeserializer[K],
    valueDeserializer: KafkaDeserializer[V]
  ): ConsumerSettings[K, V] = create(
    keyDeserializer = keyDeserializer,
    valueDeserializer = valueDeserializer,
    executionContext = None
  )

  /**
    * Creates a new [[ConsumerSettings]] instance using
    * implicit [[Deserializer]]s for the key and value.
    */
  def apply[K, V](
    implicit keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V]
  ): ConsumerSettings[K, V] = create(
    keyDeserializer = keyDeserializer,
    valueDeserializer = valueDeserializer,
    executionContext = None
  )

  /**
    * Creates a new [[ConsumerSettings]] instance using the
    * specified settings. Since offset commits are managed
    * manually, automatic commits are disabled by default.
    * Automatic offset commits can be enabled again using
    * [[ConsumerSettings#withEnableAutoCommit]].<br>
    * <br>
    * Since some Kafka operations are blocking, these should
    * be run on a dedicated `ExecutionContext`. If you have
    * a suitable context, you can specify it. Otherwise,
    * you can:<br>
    * <br>
    * - use `consumerExecutionContextResource` to create one,<br>
    * <br>
    * - not specify an `ExecutionContext`, and a default one
    *   will be used; the default context is equivalent to a
    *   `consumerExecutionContextResource` with `1` thread.
    */
  def apply[K, V](
    keyDeserializer: KafkaDeserializer[K],
    valueDeserializer: KafkaDeserializer[V],
    executionContext: ExecutionContext
  ): ConsumerSettings[K, V] = create(
    keyDeserializer = keyDeserializer,
    valueDeserializer = valueDeserializer,
    executionContext = Some(executionContext)
  )

  /**
    * Creates a new [[ConsumerSettings]] instance using
    * the specified `ExecutionContext` and implicit
    * [[Deserializer]]s for the key and value.
    */
  def apply[K, V](executionContext: ExecutionContext)(
    implicit keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V]
  ): ConsumerSettings[K, V] = create(
    keyDeserializer = keyDeserializer,
    valueDeserializer = valueDeserializer,
    executionContext = Some(executionContext)
  )

  implicit def consumerSettingsShow[K, V]: Show[ConsumerSettings[K, V]] =
    Show.show { s =>
      s"ConsumerSettings(closeTimeout = ${s.closeTimeout}, commitTimeout = ${s.commitTimeout}, pollInterval = ${s.pollInterval}, pollTimeout = ${s.pollTimeout}, commitRecovery = ${s.commitRecovery}, consumerFactory = ${s.consumerFactory})"
    }
}

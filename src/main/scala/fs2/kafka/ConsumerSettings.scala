/*
 * Copyright 2018 OVO Energy Ltd
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
import org.apache.kafka.common.serialization.Deserializer

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

sealed abstract class ConsumerSettings[K, V] {
  def keyDeserializer: Deserializer[K]

  def valueDeserializer: Deserializer[V]

  def executionContext: ExecutionContext

  def properties: Map[String, String]

  def withBootstrapServers(bootstrapServers: String): ConsumerSettings[K, V]

  def withAutoOffsetReset(autoOffsetReset: AutoOffsetReset): ConsumerSettings[K, V]

  def withClientId(clientId: String): ConsumerSettings[K, V]

  def withGroupId(groupId: String): ConsumerSettings[K, V]

  def withMaxPollRecords(maxPollRecords: Int): ConsumerSettings[K, V]

  def withMaxPollInterval(maxPollInterval: FiniteDuration): ConsumerSettings[K, V]

  def withSessionTimeout(sessionTimeout: FiniteDuration): ConsumerSettings[K, V]

  def withHeartbeatInterval(heartbeatInterval: FiniteDuration): ConsumerSettings[K, V]

  def withEnableAutoCommit(enableAutoCommit: Boolean): ConsumerSettings[K, V]

  def withAutoCommitInterval(autoCommitInterval: FiniteDuration): ConsumerSettings[K, V]

  def withProperty(key: String, value: String): ConsumerSettings[K, V]

  def withProperties(properties: (String, String)*): ConsumerSettings[K, V]

  def withProperties(properties: Map[String, String]): ConsumerSettings[K, V]

  def closeTimeout: FiniteDuration

  def withCloseTimeout(closeTimeout: FiniteDuration): ConsumerSettings[K, V]

  def commitTimeout: FiniteDuration

  def withCommitTimeout(commitTimeout: FiniteDuration): ConsumerSettings[K, V]

  def fetchTimeout: FiniteDuration

  def withFetchTimeout(fetchTimeout: FiniteDuration): ConsumerSettings[K, V]

  def pollInterval: FiniteDuration

  def withPollInterval(pollInterval: FiniteDuration): ConsumerSettings[K, V]

  def pollTimeout: FiniteDuration

  def withPollTimeout(pollTimeout: FiniteDuration): ConsumerSettings[K, V]

  def commitRecovery: CommitRecovery

  def withCommitRecovery(commitRecovery: CommitRecovery): ConsumerSettings[K, V]
}

object ConsumerSettings {
  private[this] final case class ConsumerSettingsImpl[K, V](
    override val keyDeserializer: Deserializer[K],
    override val valueDeserializer: Deserializer[V],
    override val executionContext: ExecutionContext,
    override val properties: Map[String, String],
    override val closeTimeout: FiniteDuration,
    override val commitTimeout: FiniteDuration,
    override val fetchTimeout: FiniteDuration,
    override val pollInterval: FiniteDuration,
    override val pollTimeout: FiniteDuration,
    override val commitRecovery: CommitRecovery
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

    override def withFetchTimeout(fetchTimeout: FiniteDuration): ConsumerSettings[K, V] =
      copy(fetchTimeout = fetchTimeout)

    override def withPollInterval(pollInterval: FiniteDuration): ConsumerSettings[K, V] =
      copy(pollInterval = pollInterval)

    override def withPollTimeout(pollTimeout: FiniteDuration): ConsumerSettings[K, V] =
      copy(pollTimeout = pollTimeout)

    override def withCommitRecovery(commitRecovery: CommitRecovery): ConsumerSettings[K, V] =
      copy(commitRecovery = commitRecovery)

    override def toString: String =
      Show[ConsumerSettings[K, V]].show(this)
  }

  /**
    * Creates a new [[ConsumerSettings]] instance using the specified
    * settings. Since offset commits are managed manually, automatic
    * commits are disabled by default. You can explicitly enable it
    * again using [[ConsumerSettings.withEnableAutoCommit]].<br>
    * <br>
    * Since some Kafka operations are blocking, we should shift these
    * operations to a dedicated `ExecutionContext.` A sensible option
    * is provided by the [[consumerExecutionContextStream]] function.
    */
  def apply[K, V](
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V],
    executionContext: ExecutionContext
  ): ConsumerSettings[K, V] = ConsumerSettingsImpl(
    keyDeserializer = keyDeserializer,
    valueDeserializer = valueDeserializer,
    executionContext = executionContext,
    properties = Map(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"),
    closeTimeout = 20.seconds,
    commitTimeout = 15.seconds,
    fetchTimeout = 500.millis,
    pollInterval = 50.millis,
    pollTimeout = 50.millis,
    commitRecovery = CommitRecovery.Default
  )

  implicit def consumerSettingsShow[K, V]: Show[ConsumerSettings[K, V]] =
    Show.show { s =>
      s"ConsumerSettings(closeTimeout = ${s.closeTimeout}, commitTimeout = ${s.commitTimeout}, fetchTimeout = ${s.fetchTimeout}, pollInterval = ${s.pollInterval}, pollTimeout = ${s.pollTimeout}, commitRecovery = ${s.commitRecovery})"
    }
}

package fs2.kafka
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Deserializer

import scala.concurrent.duration._

sealed abstract class ConsumerSettings[K, V] {
  def keyDeserializer: Deserializer[K]

  def valueDeserializer: Deserializer[V]

  def nativeSettings: Map[String, AnyRef]

  def closeTimeout: FiniteDuration

  def commitTimeout: FiniteDuration

  def pollInterval: FiniteDuration

  def pollTimeout: FiniteDuration
}

object ConsumerSettings {
  private[this] final class ConsumerSettingsImpl[K, V](
    override val keyDeserializer: Deserializer[K],
    override val valueDeserializer: Deserializer[V],
    override val nativeSettings: Map[String, AnyRef],
    override val closeTimeout: FiniteDuration,
    override val commitTimeout: FiniteDuration,
    override val pollInterval: FiniteDuration,
    override val pollTimeout: FiniteDuration
  ) extends ConsumerSettings[K, V]

  private[this] val defaultNativeSettings: Map[String, AnyRef] =
    Map(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false")

  /**
    * Creates a new [[ConsumerSettings]] instance using the specified
    * settings. Since offset commits are managed manually, automatic
    * offset commits are disabled by default. You can explicitly
    * enable automatic offset commits by passing:
    *
    * {{{
    * ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true"
    * }}}
    *
    * in the `nativeSettings` argument to this function.
    */
  def apply[K, V](
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V],
    nativeSettings: Map[String, AnyRef],
    closeTimeout: FiniteDuration = 20.seconds,
    commitTimeout: FiniteDuration = 15.seconds,
    pollInterval: FiniteDuration = 50.millis,
    pollTimeout: FiniteDuration = 50.millis
  ): ConsumerSettings[K, V] = new ConsumerSettingsImpl(
    keyDeserializer = keyDeserializer,
    valueDeserializer = valueDeserializer,
    nativeSettings = defaultNativeSettings ++ nativeSettings,
    closeTimeout = closeTimeout,
    commitTimeout = commitTimeout,
    pollInterval = pollInterval,
    pollTimeout = pollTimeout
  )
}

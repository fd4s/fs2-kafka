package fs2.kafka

import org.apache.kafka.common.serialization.Serializer

import scala.concurrent.duration._

sealed abstract class ProducerSettings[K, V] {
  def keySerializer: Serializer[K]

  def valueSerializer: Serializer[V]

  def nativeSettings: Map[String, AnyRef]

  def closeTimeout: FiniteDuration
}

object ProducerSettings {
  private[this] final class ProducerSettingsImpl[K, V](
    override val keySerializer: Serializer[K],
    override val valueSerializer: Serializer[V],
    override val nativeSettings: Map[String, AnyRef],
    override val closeTimeout: FiniteDuration
  ) extends ProducerSettings[K, V]

  def apply[K, V](
    keySerializer: Serializer[K],
    valueSerializer: Serializer[V],
    nativeSettings: Map[String, AnyRef],
    closeTimeout: FiniteDuration = 60.seconds
  ): ProducerSettings[K, V] =
    new ProducerSettingsImpl(
      keySerializer = keySerializer,
      valueSerializer = valueSerializer,
      nativeSettings = nativeSettings,
      closeTimeout = closeTimeout
    )
}

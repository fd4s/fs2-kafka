package fs2.kafka

import cats.Show
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serializer

import scala.concurrent.duration._

sealed abstract class ProducerSettings[K, V] {
  def keySerializer: Serializer[K]

  def valueSerializer: Serializer[V]

  def properties: Map[String, String]

  def withBootstrapServers(bootstrapServers: String): ProducerSettings[K, V]

  def withAcks(acks: Acks): ProducerSettings[K, V]

  def withBatchSize(batchSize: Int): ProducerSettings[K, V]

  def withClientId(clientId: String): ProducerSettings[K, V]

  def withRetries(retries: Int): ProducerSettings[K, V]

  def withMaxInFlightRequestsPerConnection(
    maxInFlightRequestsPerConnection: Int
  ): ProducerSettings[K, V]

  def withEnableIdempotence(enableIdempotence: Boolean): ProducerSettings[K, V]

  def withProperty(key: String, value: String): ProducerSettings[K, V]

  def withProperties(properties: (String, String)*): ProducerSettings[K, V]

  def withProperties(properties: Map[String, String]): ProducerSettings[K, V]

  def closeTimeout: FiniteDuration

  def withCloseTimeout(closeTimeout: FiniteDuration): ProducerSettings[K, V]
}

object ProducerSettings {
  private[this] final case class ProducerSettingsImpl[K, V](
    override val keySerializer: Serializer[K],
    override val valueSerializer: Serializer[V],
    override val properties: Map[String, String],
    override val closeTimeout: FiniteDuration
  ) extends ProducerSettings[K, V] {
    override def withBootstrapServers(bootstrapServers: String): ProducerSettings[K, V] =
      withProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

    override def withAcks(acks: Acks): ProducerSettings[K, V] =
      withProperty(ProducerConfig.ACKS_CONFIG, acks match {
        case Acks.ZeroAcks => "0"
        case Acks.OneAcks  => "1"
        case Acks.AllAcks  => "all"
      })

    override def withBatchSize(batchSize: Int): ProducerSettings[K, V] =
      withProperty(ProducerConfig.BATCH_SIZE_CONFIG, batchSize.toString)

    override def withClientId(clientId: String): ProducerSettings[K, V] =
      withProperty(ProducerConfig.CLIENT_ID_CONFIG, clientId)

    override def withRetries(retries: Int): ProducerSettings[K, V] =
      withProperty(ProducerConfig.RETRIES_CONFIG, retries.toString)

    override def withMaxInFlightRequestsPerConnection(
      maxInFlightRequestsPerConnection: Int
    ): ProducerSettings[K, V] =
      withProperty(
        ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
        maxInFlightRequestsPerConnection.toString
      )

    override def withEnableIdempotence(enableIdempotence: Boolean): ProducerSettings[K, V] =
      withProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotence.toString)

    override def withProperty(key: String, value: String): ProducerSettings[K, V] =
      copy(properties = properties.updated(key, value))

    override def withProperties(properties: (String, String)*): ProducerSettings[K, V] =
      copy(properties = this.properties ++ properties.toMap)

    override def withProperties(properties: Map[String, String]): ProducerSettings[K, V] =
      copy(properties = this.properties ++ properties)

    override def withCloseTimeout(closeTimeout: FiniteDuration): ProducerSettings[K, V] =
      copy(closeTimeout = closeTimeout)

    override def toString: String =
      Show[ProducerSettings[K, V]].show(this)
  }

  def apply[K, V](
    keySerializer: Serializer[K],
    valueSerializer: Serializer[V]
  ): ProducerSettings[K, V] =
    ProducerSettingsImpl(
      keySerializer = keySerializer,
      valueSerializer = valueSerializer,
      properties = Map.empty,
      closeTimeout = 60.seconds
    )

  implicit def producerSettingsShow[K, V]: Show[ProducerSettings[K, V]] =
    Show.show(s => s"ProducerSettings(closeTimeout = ${s.closeTimeout})")
}

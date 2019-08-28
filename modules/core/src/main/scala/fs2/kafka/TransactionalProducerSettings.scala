package fs2.kafka

import cats.Show
import cats.effect.Sync
import org.apache.kafka.clients.producer.ProducerConfig

import scala.concurrent.duration.FiniteDuration

/**
  * [[TransactionalProducerSettings]] contain settings necessary to create a
  * [[TransactionalKafkaProducer]]. At the very least, this includes a transactional
  * ID and a collection of "standard" [[ProducerSettings]].<br>
  * <br>
  * [[TransactionalProducerSettings]] instances are immutable and all modification
  * functions return a new [[TransactionalProducerSettings]] instance.<br>
  * <br>
  * Use `TransactionalProducerSettings#apply` to create a new instance.
  */
sealed abstract class TransactionalProducerSettings[F[_], K, V] {

  /**
    * The ID which should be used in all transactions. Populates
    * this property when building the underlying Kafka producer.
    *
    * {{{
    * ProducerConfig.TRANSACTIONAL_ID_CONFIG
    * }}}
    */
  def transactionalId: String

  /**
    * Settings unrelated to transactions which should be used to
    * configure the Kafka producer.
    */
  def baseSettings: ProducerSettings[F, K, V]

  /**
    * Returns a new [[TransactionalProducerSettings]] instance with the
    * specified transaction timeout. This is equivalent to setting the following
    * property using the `withProperty` function on `baseSettings`, except
    * you can specify it after constructing the [[TransactionalProducerSettings]]
    * instance and using a [[FiniteDuration]] instead of a `String`.
    *
    * {{{
    * ProducerConfig.TRANSACTION_TIMEOUT_CONFIG
    * }}}
    */
  def withTransactionTimeout(timeout: FiniteDuration): TransactionalProducerSettings[F, K, V]

  /**
    * Creates a new `Producer`, and initializes its transactional mode.
    * Note that this operation should be bracketed, e.g. using `Resource`,
    * to ensure the `close` function on the producer is called.
    */
  def createProducer: F[KafkaByteProducer]

  override def toString: String =
    s"TransactionalProducerSettings(transactionalId = $transactionalId)"
}

object TransactionalProducerSettings {
  private[this] final case class TransactionalProducerSettingsImpl[F[_], K, V](
    override val transactionalId: String,
    override val baseSettings: ProducerSettings[F, K, V]
  )(implicit F: Sync[F])
      extends TransactionalProducerSettings[F, K, V] {
    override def withTransactionTimeout(
      timeout: FiniteDuration
    ): TransactionalProducerSettings[F, K, V] = copy(
      baseSettings = baseSettings
        .withProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, timeout.toMillis.toString)
    )

    override def createProducer: F[KafkaByteProducer] =
      F.flatTap(baseSettings.createProducer) { producer =>
        F.delay(producer.initTransactions())
      }
  }

  def apply[F[_], K, V](id: String, settings: ProducerSettings[F, K, V])(
    implicit F: Sync[F]
  ): TransactionalProducerSettings[F, K, V] = {
    TransactionalProducerSettingsImpl(
      id,
      settings.withProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, id)
    )
  }

  implicit def transationalSettingsShow[F[_], K, V]: Show[TransactionalProducerSettings[F, K, V]] =
    Show.fromToString
}

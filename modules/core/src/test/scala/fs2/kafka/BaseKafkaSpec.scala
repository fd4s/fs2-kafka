package fs2.kafka

import cats.effect.{IO, Sync}
import fs2.kafka.internal.converters.collection._
import java.util.UUID

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer => KConsumer}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import scala.concurrent.duration._

abstract class BaseKafkaSpec extends BaseAsyncSpec with EmbeddedKafka {
  implicit final val stringSerializer: KafkaSerializer[String] =
    new org.apache.kafka.common.serialization.StringSerializer

  implicit final val stringDeserializer: KafkaDeserializer[String] =
    new org.apache.kafka.common.serialization.StringDeserializer

  final val transactionTimeoutInterval: FiniteDuration = 1.second

  final def adminClientSettings(
    config: EmbeddedKafkaConfig
  ): AdminClientSettings[IO] =
    AdminClientSettings[IO]
      .withProperties(adminClientProperties(config))

  final def consumerSettings[F[_]](
    config: EmbeddedKafkaConfig
  )(implicit F: Sync[F]): ConsumerSettings[F, String, String] =
    ConsumerSettings[F, String, String]
      .withProperties(consumerProperties(config))
      .withRecordMetadata(_.timestamp.toString)

  final def producerSettings[F[_]](
    config: EmbeddedKafkaConfig
  )(implicit F: Sync[F]): ProducerSettings[F, String, String] =
    ProducerSettings[F, String, String]
      .withProperties(producerProperties(config))

  final def adminClientProperties(config: EmbeddedKafkaConfig): Map[String, String] =
    Map(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}")

  final def consumerProperties(config: EmbeddedKafkaConfig): Map[String, String] =
    Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.GROUP_ID_CONFIG -> "group"
    )

  final def producerProperties(config: EmbeddedKafkaConfig): Map[String, String] =
    Map(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}")

  final def withKafka[A](f: (EmbeddedKafkaConfig, String) => A): A =
    withRunningKafkaOnFoundPort(
      EmbeddedKafkaConfig(
        customBrokerProperties = Map(
          "transaction.state.log.replication.factor" -> "1",
          "transaction.abort.timed.out.transaction.cleanup.interval.ms" -> transactionTimeoutInterval.toMillis.toString
        )
      )
    )(f(_, nextTopicName()))

  final def withKafkaConsumer(
    nativeSettings: Map[String, AnyRef]
  ): WithKafkaConsumer =
    new WithKafkaConsumer(nativeSettings)

  final class WithKafkaConsumer(
    nativeSettings: Map[String, AnyRef]
  ) {
    def apply[A](f: KConsumer[Array[Byte], Array[Byte]] => A): A = {
      val consumer: KConsumer[Array[Byte], Array[Byte]] =
        new KConsumer[Array[Byte], Array[Byte]](
          nativeSettings.asJava,
          new ByteArrayDeserializer,
          new ByteArrayDeserializer
        )

      try f(consumer)
      finally consumer.close()
    }
  }

  private[this] def nextTopicName(): String =
    s"topic-${UUID.randomUUID()}"
}

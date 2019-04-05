package fs2.kafka

import java.util.UUID

import cats.effect.{Sync, IO}
import fs2.Stream
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer => KConsumer}
import org.apache.kafka.clients.producer.ProducerConfig

import scala.collection.JavaConverters._

abstract class BaseKafkaSpec extends BaseAsyncSpec with EmbeddedKafka {
  implicit final val stringSerializer: KafkaSerializer[String] =
    new org.apache.kafka.common.serialization.StringSerializer

  implicit final val stringDeserializer: KafkaDeserializer[String] =
    new org.apache.kafka.common.serialization.StringDeserializer

  final def adminClientSettings(
    config: EmbeddedKafkaConfig
  ): AdminClientSettings =
    AdminClientSettings.Default
      .withProperties(adminClientProperties(config))

  final def consumerSettings(
    config: EmbeddedKafkaConfig
  ): ConsumerSettings[String, String] =
    ConsumerSettings[String, String]
      .withProperties(consumerProperties(config))
      .withRecordMetadata(_.timestamp.toString)

  final def consumerSettingsExecutionContext(
    config: EmbeddedKafkaConfig
  ): Stream[IO, ConsumerSettings[String, String]] =
    consumerExecutionContextStream[IO].map { executionContext =>
      ConsumerSettings[String, String](executionContext)
        .withProperties(consumerProperties(config))
        .withRecordMetadata(_.timestamp.toString)
    }

  final def producerSettings[F[_]](
    config: EmbeddedKafkaConfig
  )(implicit F: Sync[F]): ProducerSettings[F, String, String] =
    ProducerSettings[F, String, String]
      .withProperties(producerProperties(config))

  final def producerSettingsExecutionContext[F[_]](
    config: EmbeddedKafkaConfig
  ): Stream[IO, ProducerSettings[IO, String, String]] =
    producerExecutionContextStream[IO].map { executionContext =>
      ProducerSettings[IO, String, String](executionContext)
        .withProperties(producerProperties(config))
    }

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
    withRunningKafkaOnFoundPort(EmbeddedKafkaConfig())(f(_, nextTopicName()))

  final def withKafkaConsumer[K, V](
    nativeSettings: Map[String, AnyRef]
  ): WithKafkaConsumer[K, V] =
    new WithKafkaConsumer[K, V](nativeSettings)

  final class WithKafkaConsumer[K, V](
    nativeSettings: Map[String, AnyRef]
  ) {
    def apply[A](f: KConsumer[K, V] => A)(
      implicit keyDeserializer: Deserializer[K],
      valueDeserializer: Deserializer[V]
    ): A = {
      val consumer: KConsumer[K, V] =
        new KConsumer[K, V](
          nativeSettings.asJava,
          keyDeserializer,
          valueDeserializer
        )

      try f(consumer)
      finally consumer.close()
    }
  }

  private[this] def nextTopicName(): String =
    s"topic-${UUID.randomUUID()}"
}

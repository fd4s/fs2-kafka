package fs2.kafka

import java.util.UUID

import cats.effect.IO
import fs2.Stream
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer => KConsumer}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization._

import scala.collection.JavaConverters._

abstract class BaseKafkaSpec extends BaseAsyncSpec with EmbeddedKafka {
  implicit final val stringSerializer: Serializer[String] =
    new StringSerializer

  implicit final val stringDeserializer: Deserializer[String] =
    new StringDeserializer

  final def adminClientSettings(
    config: EmbeddedKafkaConfig
  ): AdminClientSettings =
    AdminClientSettings.Default
      .withProperties(adminClientProperties(config))

  final def consumerSettings(
    config: EmbeddedKafkaConfig
  ): ConsumerSettings[String, String] =
    ConsumerSettings(
      keyDeserializer = new StringDeserializer,
      valueDeserializer = new StringDeserializer
    ).withProperties(consumerProperties(config))
      .withRecordMetadata(_.timestamp.toString)

  final def consumerSettingsExecutionContext(
    config: EmbeddedKafkaConfig
  ): Stream[IO, ConsumerSettings[String, String]] =
    consumerExecutionContextStream[IO].map { executionContext =>
      ConsumerSettings(
        keyDeserializer = new StringDeserializer,
        valueDeserializer = new StringDeserializer,
        executionContext = executionContext
      ).withProperties(consumerProperties(config))
        .withRecordMetadata(_.timestamp.toString)
    }

  final def producerSettings(
    config: EmbeddedKafkaConfig
  ): ProducerSettings[String, String] =
    ProducerSettings(
      keySerializer = new StringSerializer,
      valueSerializer = new StringSerializer
    ).withProperties(producerProperties(config))

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

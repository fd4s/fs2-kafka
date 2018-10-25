package fs2.kafka

import cats.effect.IO
import fs2.Stream
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer => KConsumer}
import org.apache.kafka.common.serialization._

import scala.collection.JavaConverters._

abstract class BaseKafkaSpec extends BaseAsyncSpec with EmbeddedKafka {
  implicit final val stringSerializer: Serializer[String] =
    new StringSerializer

  implicit final val stringDeserializer: Deserializer[String] =
    new StringDeserializer

  final def consumerSettingsWith(
    config: EmbeddedKafkaConfig
  ): Stream[IO, ConsumerSettings[String, String]] =
    consumerExecutionContext[IO].map { executionContext =>
      ConsumerSettings(
        keyDeserializer = new StringDeserializer,
        valueDeserializer = new StringDeserializer,
        nativeSettings = nativeSettingsWith(config),
        executionContext = executionContext
      )
    }

  final def nativeSettingsWith(config: EmbeddedKafkaConfig): Map[String, AnyRef] =
    Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.GROUP_ID_CONFIG -> "group"
    )

  final def withKafka[A](f: (EmbeddedKafkaConfig, TopicName) => A): A =
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

  type TopicName = String

  private[this] var nextTopicNum: Int = 1

  private[this] def nextTopicName(): TopicName = {
    val n = nextTopicNum
    nextTopicNum += 1
    s"topic-$n"
  }
}

/*
rule = Fs2Kafka
 */
package fix

import fs2.kafka._
import scala.concurrent.ExecutionContext
import cats.effect.{ContextShift, IO, Resource, Timer}

object FactoryDeprecations {
  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)

  val consumerSettings: ConsumerSettings[IO, String, String] = ???
  val producerSettings: ProducerSettings[IO, String, String] = ???
  val transactionalProducerSettings: TransactionalProducerSettings[IO, String, String] = ???
  val adminClientSettings: AdminClientSettings[IO] = ???

  KafkaProducer.resource[IO].using(producerSettings)
  KafkaProducer.stream[IO].using(producerSettings)

  TransactionalKafkaProducer.resource[IO].using(transactionalProducerSettings)
  TransactionalKafkaProducer.stream[IO].using(transactionalProducerSettings)

  KafkaConsumer.resource[IO].using(consumerSettings)
  KafkaConsumer.stream[IO].using(consumerSettings)
  fs2.kafka.KafkaConsumer.stream[IO].using(consumerSettings)
}

package fix

import scala.concurrent.ExecutionContext

import cats.effect.{ContextShift, IO, Resource, Timer}
import fs2.kafka._

object FactoryDeprecations {

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val timer: Timer[IO]     = IO.timer(ExecutionContext.global)

  val consumerSettings: ConsumerSettings[IO, String, String]                           = ???
  val producerSettings: ProducerSettings[IO, String, String]                           = ???
  val transactionalProducerSettings: TransactionalProducerSettings[IO, String, String] = ???
  val adminClientSettings: AdminClientSettings[IO]                                     = ???

  KafkaProducer[IO].resource(producerSettings)
  KafkaProducer[IO].stream(producerSettings)

  TransactionalKafkaProducer[IO].resource(transactionalProducerSettings)
  TransactionalKafkaProducer[IO].stream(transactionalProducerSettings)

  KafkaConsumer[IO].resource(consumerSettings)
  KafkaConsumer[IO].stream(consumerSettings)
  fs2.kafka.KafkaConsumer[IO].stream(consumerSettings)

}

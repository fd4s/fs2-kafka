package fix

import fs2.kafka._
import scala.concurrent.ExecutionContext
import cats.effect.{ContextShift, IO, Resource, Timer}
import fs2.kafka.{ KafkaAdminClient, KafkaConsumer, KafkaProducer, TransactionalKafkaProducer }

object Fs2Kafka {
  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)

  val consumerSettings: ConsumerSettings[IO, String, String] = ???
  val producerSettings: ProducerSettings[IO, String, String] = ???
  val transactionalProducerSettings: TransactionalProducerSettings[IO, String, String] = ???
  val adminClientSettings: AdminClientSettings[IO] = ???

  KafkaProducer.resource[IO].using(producerSettings)
  KafkaProducer.resource(producerSettings)

  KafkaProducer.stream[IO].using(producerSettings)
  KafkaProducer.stream(producerSettings)

  KafkaProducer.pipe(producerSettings)

  TransactionalKafkaProducer.resource[IO].using(transactionalProducerSettings)
  TransactionalKafkaProducer.resource(transactionalProducerSettings)

  TransactionalKafkaProducer.stream[IO].using(transactionalProducerSettings)
  TransactionalKafkaProducer.stream(transactionalProducerSettings)

  KafkaConsumer.resource[IO].using(consumerSettings)
  KafkaConsumer.resource(consumerSettings)

  KafkaConsumer.stream[IO].using(consumerSettings)
  KafkaConsumer.stream(consumerSettings)

  KafkaAdminClient.resource(adminClientSettings)
  KafkaAdminClient.stream(adminClientSettings)
  
  fs2.kafka.KafkaConsumer.resource[IO].using(consumerSettings)
  Resource.liftF(IO(consumerSettings)).flatMap(KafkaConsumer.resource(_))
  Resource.liftF(IO(consumerSettings)).flatMap(KafkaConsumer.resource[IO].using(_))
}

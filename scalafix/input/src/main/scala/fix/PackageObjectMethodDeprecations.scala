/*
rule = Fs2Kafka
 */
package fix

import fs2.kafka.{ producerResource, _ }
import scala.concurrent.ExecutionContext
import cats.effect.{ContextShift, IO, Resource, Timer}

object Fs2Kafka {
  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)

  val consumerSettings: ConsumerSettings[IO, String, String] = ???
  val producerSettings: ProducerSettings[IO, String, String] = ???
  val transactionalProducerSettings: TransactionalProducerSettings[IO, String, String] = ???
  val adminClientSettings: AdminClientSettings[IO] = ???

  producerResource[IO].using(producerSettings)
  producerResource(producerSettings)

  producerStream[IO].using(producerSettings)
  producerStream(producerSettings)

  produce(producerSettings)
  
  def foo(bar: Any)(baz: Any): Any = ???
  foo(produce(producerSettings))(3)

  transactionalProducerResource[IO].using(transactionalProducerSettings)
  transactionalProducerResource(transactionalProducerSettings)

  transactionalProducerStream[IO].using(transactionalProducerSettings)
  transactionalProducerStream(transactionalProducerSettings)

  consumerResource[IO].using(consumerSettings)
  consumerResource(consumerSettings)

  consumerStream[IO].using(consumerSettings)
  consumerStream(consumerSettings)

  adminClientResource(adminClientSettings)
  adminClientStream(adminClientSettings)
  
  fs2.kafka.consumerResource[IO].using(consumerSettings)
  Resource.liftF(IO(consumerSettings)).flatMap(consumerResource(_))
  Resource.liftF(IO(consumerSettings)).flatMap(consumerResource[IO].using(_))
}

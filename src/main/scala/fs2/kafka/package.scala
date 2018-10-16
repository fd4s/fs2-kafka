package fs2

import java.util.concurrent.{Executors, ThreadFactory}

import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}

import scala.concurrent.ExecutionContext

package object kafka {
  def consumerStream[F[_], K, V](settings: ConsumerSettings[K, V])(
    implicit F: ConcurrentEffect[F],
    context: ContextShift[F],
    timer: Timer[F]
  ): Stream[F, KafkaConsumer[F, K, V]] =
    KafkaConsumer.consumerStream(settings)

  def consumerStream[F[_]]: ConsumerStream[F] =
    new ConsumerStream[F]

  def consumerExecutionContext[F[_]](implicit F: Sync[F]): Stream[F, ExecutionContext] =
    Stream
      .bracket {
        F.delay {
          Executors.newSingleThreadExecutor(new ThreadFactory {
            override def newThread(runnable: Runnable): Thread = {
              val thread = new Thread(runnable)
              thread.setName("kafka-consumer")
              thread.setDaemon(true)
              thread
            }
          })
        }
      }(executor => F.delay(executor.shutdown()))
      .map(ExecutionContext.fromExecutor)

  def producerStream[F[_], K, V](settings: ProducerSettings[K, V])(
    implicit F: ConcurrentEffect[F]
  ): Stream[F, KafkaProducer[F, K, V]] =
    KafkaProducer.producerStream(settings)

  def producerStream[F[_]]: ProducerStream[F] =
    new ProducerStream[F]
}

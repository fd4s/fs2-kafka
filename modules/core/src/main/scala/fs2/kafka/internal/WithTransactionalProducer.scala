/*
 * Copyright 2018-2022 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.effect.concurrent.Semaphore
import cats.effect.{Concurrent, ContextShift, Resource}
import cats.implicits._
import fs2.kafka.internal.syntax._
import fs2.kafka.{KafkaByteProducer, TransactionalProducerSettings}

private[kafka] sealed abstract class WithTransactionalProducer[F[_]] {
  def apply[A](f: (KafkaByteProducer, Blocking[F], ExclusiveAccess[F, A]) => F[A]): F[A]

  def exclusiveAccess[A](f: (KafkaByteProducer, Blocking[F]) => F[A]): F[A] = apply {
    case (producer, blocking, exclusive) => exclusive(f(producer, blocking))
  }

  def blocking[A](f: KafkaByteProducer => A): F[A] = apply {
    case (producer, blocking, _) => blocking(f(producer))
  }
}

private[kafka] object WithTransactionalProducer {
  def apply[F[_], K, V](
    settings: TransactionalProducerSettings[F, K, V]
  )(
    implicit F: Concurrent[F],
    context: ContextShift[F]
  ): Resource[F, WithTransactionalProducer[F]] =
    (blockingResource(settings.producerSettings), Resource.eval(Semaphore(1))).tupled.flatMap {
      case (blocking, accessSemaphore) =>
        Resource[F, WithTransactionalProducer[F]] {
          settings.producerSettings.createProducer.flatMap { producer =>
            val withProducer = create(producer, blocking, accessSemaphore)

            val initTransactions = withProducer.blocking { _.initTransactions() }

            val close = withProducer.exclusiveAccess {
              case (producer, blocking) =>
                blocking(producer.close(settings.producerSettings.closeTimeout.asJava))
            }

            initTransactions.as((withProducer, close))
          }

        }
    }

  private def create[F[_]](
    producer: KafkaByteProducer,
    _blocking: Blocking[F],
    transactionSemaphore: Semaphore[F]
  ): WithTransactionalProducer[F] = new WithTransactionalProducer[F] {
    override def apply[A](
      f: (KafkaByteProducer, Blocking[F], ExclusiveAccess[F, A]) => F[A]
    ): F[A] =
      f(producer, _blocking, transactionSemaphore.withPermit)
  }
}

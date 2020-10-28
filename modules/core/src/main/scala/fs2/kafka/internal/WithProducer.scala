/*
 * Copyright 2018-2020 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.effect.{Blocker, ContextShift, Resource, Sync}
import cats.{~>, Id}
import cats.implicits._
import fs2.kafka.{KafkaByteProducer, ProducerSettings, TransactionalProducerSettings}
import fs2.kafka.internal.syntax._

private[kafka] sealed abstract class WithProducer[F[_]] {
  def apply[A](f: (KafkaByteProducer, Id ~> F) => F[A]): F[A]
  def blocking[A](f: KafkaByteProducer => A): F[A]
}

private[kafka] object WithProducer {
  def apply[F[_], K, V](
    settings: ProducerSettings[F, K, V]
  )(
    implicit F: Sync[F],
    context: ContextShift[F]
  ): Resource[F, WithProducer[F]] =
    blockerResource(settings).flatMap { blocker =>
      Resource.make(
        settings.createProducer.map(create(_, blocker))
      )(_.blocking { _.close(settings.closeTimeout.asJava) })
    }

  def apply[F[_], K, V](
    settings: TransactionalProducerSettings[F, K, V]
  )(
    implicit F: Sync[F],
    context: ContextShift[F]
  ): Resource[F, WithProducer[F]] =
    blockerResource(settings.producerSettings).flatMap { blocker =>
      Resource[F, WithProducer[F]] {
        settings.producerSettings.createProducer.flatMap { producer =>
          val withProducer = create(producer, blocker)

          val initTransactions = withProducer.blocking { _.initTransactions() }

          val close = withProducer.blocking {
            _.close(settings.producerSettings.closeTimeout.asJava)
          }

          initTransactions.as((withProducer, close))
        }

      }
    }

  private def blockerResource[F[_]: Sync](
    settings: ProducerSettings[F, _, _]
  ): Resource[F, Blocker] =
    settings.blocker
      .map(Resource.pure[F, Blocker])
      .getOrElse(Blockers.producer)

  private def create[F[_]: Sync: ContextShift](
    producer: KafkaByteProducer,
    blocker: Blocker
  ): WithProducer[F] = new WithProducer[F] {
    private val blockingK = new ~>[Id, F] {
      override def apply[A](a: A) = blocker.delay(a)
    }

    override def apply[A](f: (KafkaByteProducer, Id ~> F) => F[A]): F[A] =
      f(producer, blockingK)

    override def blocking[A](f: KafkaByteProducer => A): F[A] =
      blocker.delay(f(producer))
  }
}

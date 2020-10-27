/*
 * Copyright 2018-2020 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.effect.{Blocker, ContextShift, Resource, Sync}
import cats.implicits._
import fs2.kafka.{KafkaByteProducer, ProducerSettings, TransactionalProducerSettings}
import fs2.kafka.internal.syntax._

private[kafka] sealed abstract class WithProducer[F[_]] {
  def apply[A](f: KafkaByteProducer => F[A]): F[A]
  def blocking[A](f: KafkaByteProducer => A): F[A]
}

private[kafka] object WithProducer {
  def apply[F[_], K, V](
    settings: ProducerSettings[F, K, V]
  )(
    implicit F: Sync[F],
    context: ContextShift[F]
  ): Resource[F, WithProducer[F]] = {
    val blockerResource =
      settings.blocker
        .map(Resource.pure[F, Blocker])
        .getOrElse(Blockers.producer)

    blockerResource.flatMap { blocker =>
      Resource.make {
        settings.createProducer.map { producer =>
          new WithProducer[F] {
            override def apply[A](f: KafkaByteProducer => F[A]): F[A] =
              blocker.blockOn { f(producer) }

            override def blocking[A](f: KafkaByteProducer => A): F[A] =
              apply(producer => F.delay(f(producer)))
          }
        }
      }(_.apply { producer =>
        F.delay(producer.close(settings.closeTimeout.asJava))
      })
    }
  }

  def apply[F[_], K, V](
    settings: TransactionalProducerSettings[F, K, V]
  )(
    implicit F: Sync[F],
    context: ContextShift[F]
  ): Resource[F, WithProducer[F]] = {
    val blockerResource =
      settings.producerSettings.blocker
        .map(Resource.pure[F, Blocker])
        .getOrElse(Blockers.producer)

    blockerResource.flatMap { blocker =>
      Resource[F, WithProducer[F]] {
        settings.producerSettings.createProducer.flatMap { producer =>
          val withProducer =
            new WithProducer[F] {
              override def apply[A](f: KafkaByteProducer => F[A]): F[A] =
                blocker.blockOn { f(producer) }

              override def blocking[A](f: KafkaByteProducer => A): F[A] =
                apply(producer => F.delay(f(producer)))
            }

          val initTransactions =
            withProducer.blocking { _.initTransactions() }

          val close =
            withProducer.blocking { _.close(settings.producerSettings.closeTimeout.asJava) }

          initTransactions.as((withProducer, close))
        }
      }
    }
  }
}

/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.effect.{Blocker, ContextShift, Resource, Sync}
import cats.implicits._
import fs2.kafka.{ProducerSettings, TransactionalProducerSettings}
import fs2.kafka.internal.syntax._

private[kafka] sealed abstract class WithTransactionalProducer[F[_]] {
  def apply[A](f: (JavaByteProducer, Blocking[F]) => F[A]): F[A]

  def blocking[A](f: JavaByteProducer => A): F[A] = apply {
    case (producer, blocking) => blocking(f(producer))
  }
}

private[kafka] object WithTransactionalProducer {

  def apply[F[_], K, V](
    settings: TransactionalProducerSettings[F, K, V]
  )(
    implicit F: Sync[F],
    context: ContextShift[F]
  ): Resource[F, WithTransactionalProducer[F]] =
    blockingResource(settings.producerSettings).flatMap { blocking =>
      Resource[F, WithTransactionalProducer[F]] {
        settings.producerSettings.createProducer.flatMap { producer =>
          val withProducer = create(producer, blocking)

          val initTransactions = withProducer.blocking { _.initTransactions() }

          val close = withProducer.blocking {
            _.close(settings.producerSettings.closeTimeout.asJava)
          }

          initTransactions.as((withProducer, close))
        }

      }
    }

  private def blockingResource[F[_]: Sync: ContextShift](
    settings: ProducerSettings[F, _, _]
  ): Resource[F, Blocking[F]] =
    settings.blocker
      .map(Resource.pure[F, Blocker])
      .getOrElse(Blockers.producer)
      .map(Blocking.fromBlocker[F])

  private def create[F[_]](
    producer: JavaByteProducer,
    _blocking: Blocking[F]
  ): WithTransactionalProducer[F] = new WithTransactionalProducer[F] {
    override def apply[A](f: (JavaByteProducer, Blocking[F]) => F[A]): F[A] =
      f(producer, _blocking)
  }
}

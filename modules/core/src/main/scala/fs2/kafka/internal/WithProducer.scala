/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import fs2.kafka.producer.MkProducer
import cats.effect.{Async, Resource}
import cats.syntax.all._
import fs2.kafka.{KafkaByteProducer, ProducerSettings, TransactionalProducerSettings}
import fs2.kafka.internal.syntax._

private[kafka] sealed abstract class WithProducer[F[_]] {
  def apply[A](f: (KafkaByteProducer, Blocking[F]) => F[A]): F[A]

  def blocking[A](f: KafkaByteProducer => A): F[A] = apply {
    case (producer, blocking) => blocking(f(producer))
  }
}

private[kafka] object WithProducer {
  def apply[F[_], G[_]](
    mk: MkProducer[F],
    settings: ProducerSettings[G, _, _]
  )(
    implicit F: Async[F],
    G: Async[G]
  ): Resource[F, WithProducer[G]] = {
    val blockingF =
      settings.customBlockingContext.fold(Blocking.fromSync[F])(Blocking.fromExecutionContext[F])
    val blockingG =
      settings.customBlockingContext.fold(Blocking.fromSync[G])(Blocking.fromExecutionContext[G])

    Resource
      .make(
        mk(settings)
      )(producer => blockingF { producer.close(settings.closeTimeout.asJava) })
      .map(create(_, blockingG))
  }

  def apply[F[_], K, V](
    mk: MkProducer[F],
    settings: TransactionalProducerSettings[F, K, V]
  )(
    implicit F: Async[F]
  ): Resource[F, WithProducer[F]] =
    Resource[F, WithProducer[F]] {
      mk(settings.producerSettings).flatMap { producer =>
        val blocking = settings.producerSettings.customBlockingContext
          .fold(Blocking.fromSync[F])(Blocking.fromExecutionContext)

        val withProducer = create(producer, blocking)

        val initTransactions = withProducer.blocking { _.initTransactions() }

        val close = withProducer.blocking {
          _.close(settings.producerSettings.closeTimeout.asJava)
        }

        initTransactions.as((withProducer, close))
      }
    }

  private def create[F[_]](
    producer: KafkaByteProducer,
    _blocking: Blocking[F]
  ): WithProducer[F] = new WithProducer[F] {
    override def apply[A](f: (KafkaByteProducer, Blocking[F]) => F[A]): F[A] =
      f(producer, _blocking)
  }
}

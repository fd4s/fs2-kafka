/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import fs2.kafka.producer.MkProducer
import cats.effect.{Resource, Sync}
import cats.implicits._
import fs2.kafka.{KafkaByteProducer, ProducerSettings, TransactionalProducerSettings}
import fs2.kafka.internal.syntax._

private[kafka] sealed abstract class WithProducer[F[_]] {
  def apply[A](f: (KafkaByteProducer, Blocking[F]) => F[A]): F[A]

  def blocking[A](f: KafkaByteProducer => A): F[A] = apply {
    case (producer, blocking) => blocking(f(producer))
  }
}

private[kafka] object WithProducer {
  def apply[F[_], K, V](
    mk: MkProducer[F],
    settings: ProducerSettings[F, K, V]
  )(
    implicit F: Sync[F]
  ): Resource[F, WithProducer[F]] =
    Resource.make(
      mk(settings).map(create(_, Blocking[F]))
    )(_.blocking { _.close(settings.closeTimeout.asJava) })

  def apply[F[_], K, V](
    mk: MkProducer[F],
    settings: TransactionalProducerSettings[F, K, V]
  )(
    implicit F: Sync[F]
  ): Resource[F, WithProducer[F]] =
    Resource[F, WithProducer[F]] {
      mk(settings.producerSettings).flatMap { producer =>
        val withProducer = create(producer, Blocking[F])

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

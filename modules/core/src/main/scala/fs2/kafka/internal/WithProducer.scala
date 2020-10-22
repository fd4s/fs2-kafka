/*
 * Copyright 2018-2020 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.effect.{Resource, Sync}
import cats.implicits._
import fs2.kafka.{KafkaByteProducer, ProducerSettings, TransactionalProducerSettings}
import fs2.kafka.internal.syntax._

private[kafka] sealed abstract class WithProducer[F[_]] {
  def apply[A](f: KafkaByteProducer => F[A]): F[A]
}

private[kafka] object WithProducer {
  def apply[F[_], K, V](
    settings: ProducerSettings[F, K, V]
  )(
    implicit F: Sync[F]
  ): Resource[F, WithProducer[F]] = {
    Resource[F, WithProducer[F]] {
      settings.createProducer.map { producer =>
        val withProducer =
          new WithProducer[F] {
            override def apply[A](f: KafkaByteProducer => F[A]): F[A] =
              F.defer {
                f(producer)
              }
          }

        val close =
          withProducer { producer =>
            F.delay(producer.close(settings.closeTimeout.asJava))
          }

        (withProducer, close)
      }
    }
  }

  def apply[F[_], K, V](
    settings: TransactionalProducerSettings[F, K, V]
  )(
    implicit F: Sync[F]
  ): Resource[F, WithProducer[F]] = {
    Resource[F, WithProducer[F]] {
      settings.producerSettings.createProducer.flatMap { producer =>
        val withProducer =
          new WithProducer[F] {
            override def apply[A](f: KafkaByteProducer => F[A]): F[A] =
              F.defer {
                f(producer)
              }
          }

        val initTransactions =
          withProducer { producer =>
            F.delay(producer.initTransactions())
          }

        val close =
          withProducer { producer =>
            F.delay(producer.close(settings.producerSettings.closeTimeout.asJava))
          }

        initTransactions.as((withProducer, close))
      }
    }
  }
}

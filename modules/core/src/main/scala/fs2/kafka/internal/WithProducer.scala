/*
 * Copyright 2018-2022 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.effect.{ContextShift, Resource, Sync}
import cats.implicits._
import fs2.kafka.internal.syntax._
import fs2.kafka.{KafkaByteProducer, ProducerSettings}

private[kafka] sealed abstract class WithProducer[F[_]] {
  def apply[A](f: (KafkaByteProducer, Blocking[F]) => F[A]): F[A]

  def blocking[A](f: KafkaByteProducer => A): F[A] = apply {
    case (producer, blocking) => blocking(f(producer))
  }
}

private[kafka] object WithProducer {
  def apply[F[_], K, V](
    settings: ProducerSettings[F, K, V]
  )(
    implicit F: Sync[F],
    context: ContextShift[F]
  ): Resource[F, WithProducer[F]] =
    blockingResource(settings).flatMap { blocking =>
      Resource.make(
        settings.createProducer.map(create(_, blocking))
      )(_.blocking { _.close(settings.closeTimeout.asJava) })
    }

  private def create[F[_]](
    producer: KafkaByteProducer,
    _blocking: Blocking[F]
  ): WithProducer[F] = new WithProducer[F] {
    override def apply[A](f: (KafkaByteProducer, Blocking[F]) => F[A]): F[A] =
      f(producer, _blocking)
  }
}

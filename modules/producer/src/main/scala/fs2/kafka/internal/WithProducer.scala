/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.effect.{Blocker, ContextShift, Resource, Sync}
import cats.implicits._
import fs2.kafka.ProducerSettings
import fs2.kafka.common._
import fs2.kafka.internal.syntax._

private[kafka] sealed abstract class WithProducer[F[_]] {
  def apply[A](f: (JavaByteProducer, Blocking[F]) => F[A]): F[A]

  def blocking[A](f: JavaByteProducer => A): F[A] = apply {
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
  ): WithProducer[F] = new WithProducer[F] {
    override def apply[A](f: (JavaByteProducer, Blocking[F]) => F[A]): F[A] =
      f(producer, _blocking)
  }
}

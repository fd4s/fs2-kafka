/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.{Applicative, Functor}
import cats.syntax.all._

/**
  * Serializer which may vary depending on whether a record
  * key or value is being serialized, and which may require
  * a creation effect.
  */
sealed abstract class RecordSerializer[F[_], A] {
  def forKey: F[KeySerializer[F, A]]

  def forValue: F[ValueSerializer[F, A]]
}

object RecordSerializer {
  def apply[F[_], A](
    implicit serializer: RecordSerializer[F, A]
  ): RecordSerializer[F, A] =
    serializer

  def const[F[_]: Functor, A](
    serializer: => F[Serializer[F, A]]
  ): RecordSerializer[F, A] =
    RecordSerializer.instance(
      forKey = serializer.map(_.forKey),
      forValue = serializer.map(_.forValue)
    )

  def instance[F[_], A](
    forKey: => F[KeySerializer[F, A]],
    forValue: => F[ValueSerializer[F, A]]
  ): RecordSerializer[F, A] = {
    def _forKey = forKey
    def _forValue = forValue

    new RecordSerializer[F, A] {
      override def forKey: F[KeySerializer[F, A]] =
        _forKey

      override def forValue: F[ValueSerializer[F, A]] =
        _forValue

      override def toString: String =
        "Serializer.Record$" + System.identityHashCode(this)
    }
  }

  def lift[F[_], A](serializer: => Serializer[F, A])(
    implicit F: Applicative[F]
  ): RecordSerializer[F, A] =
    RecordSerializer.const(F.pure(serializer))
}

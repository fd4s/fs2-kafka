/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.{Applicative, Functor}
import cats.syntax.all._

sealed trait MkKeySerializer[F[_], A] {
  def forKey: F[KeySerializer[F, A]]
}

object MkKeySerializer {

  implicit def lift[F[_], A](
    implicit serializer: KeySerializer[F, A],
    F: Applicative[F]
  ): MkKeySerializer[F, A] = new MkKeySerializer[F, A] {
    override def forKey: F[KeySerializer[F, A]] = F.pure(serializer)
  }

}

sealed trait MkValueSerializer[F[_], A] {
  def forValue: F[ValueSerializer[F, A]]
}

object MkValueSerializer {
  implicit def lift[F[_], A](
    implicit serializer: ValueSerializer[F, A],
    F: Applicative[F]
  ): MkValueSerializer[F, A] = new MkValueSerializer[F, A] {
    override def forValue: F[ValueSerializer[F, A]] = F.pure(serializer)
  }
}

/**
  * Serializer which may vary depending on whether a record
  * key or value is being serialized, and which may require
  * a creation effect.
  */
sealed abstract class MkSerializers[F[_], K, V]
    extends MkKeySerializer[F, K]
    with MkValueSerializer[F, V]

object MkSerializers {
  def apply[F[_], K, V](
    implicit serializer: MkSerializers[F, K, V]
  ): MkSerializers[F, K, V] =
    serializer

  def const[F[_]: Functor, A](
    serializer: => F[Serializer[F, A]]
  ): MkSerializers[F, A, A] =
    MkSerializers.instance(
      forKey = serializer.map(_.forKey),
      forValue = serializer.map(_.forValue)
    )

  def instance[F[_], K, V](
    forKey: => F[KeySerializer[F, K]],
    forValue: => F[ValueSerializer[F, V]]
  ): MkSerializers[F, K, V] = {
    def _forKey = forKey
    def _forValue = forValue

    new MkSerializers[F, K, V] {
      override def forKey: F[KeySerializer[F, K]] =
        _forKey

      override def forValue: F[ValueSerializer[F, V]] =
        _forValue

      override def toString: String =
        "Serializer.Record$" + System.identityHashCode(this)
    }
  }

  def lift[F[_], A](serializer: => Serializer[F, A])(
    implicit F: Applicative[F]
  ): MkSerializers[F, A, A] =
    MkSerializers.const(F.pure(serializer))

  implicit def lift[F[_], A](
    implicit serializer: Serializer[F, A],
    F: Applicative[F]
  ): MkSerializers[F, A, A] =
    MkSerializers.const(F.pure(serializer))
}

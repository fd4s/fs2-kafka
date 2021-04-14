/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.{Applicative, Functor}
import cats.syntax.all._

sealed trait MkKeyDeserializer[F[_], A] {
  def forKey: F[KeyDeserializer[F, A]]
}

object MkKeyDeserializer {

  implicit def lift[F[_], A](
    implicit Deserializer: KeyDeserializer[F, A],
    F: Applicative[F]
  ): MkKeyDeserializer[F, A] = new MkKeyDeserializer[F, A] {
    override def forKey: F[KeyDeserializer[F, A]] = F.pure(Deserializer)
  }
}

sealed trait MkValueDeserializer[F[_], A] {
  def forValue: F[ValueDeserializer[F, A]]
}

object MkValueDeserializer {

  implicit def lift[F[_], A](
    implicit deserializer: ValueDeserializer[F, A],
    F: Applicative[F]
  ): MkValueDeserializer[F, A] = new MkValueDeserializer[F, A] {
    override def forValue: F[ValueDeserializer[F, A]] = F.pure(deserializer)
  }
}

/**
  * Deserializer which may vary depending on whether a record
  * key or value is being deserialized, and which may require
  * a creation effect.
  */
sealed abstract class MkDeserializers[F[_], K, V]
    extends MkKeyDeserializer[F, K]
    with MkValueDeserializer[F, V]

object MkDeserializers {
  def apply[F[_], K, V](
    implicit deserializer: MkDeserializers[F, K, V]
  ): MkDeserializers[F, K, V] =
    deserializer

  def const[F[_]: Functor, A](
    deserializer: => F[Deserializer[F, A]]
  ): MkDeserializers[F, A, A] =
    MkDeserializers.instance(
      forKey = deserializer.map(_.forKey),
      forValue = deserializer.map(_.forValue)
    )

  def instance[F[_], K, V](
    forKey: => F[KeyDeserializer[F, K]],
    forValue: => F[ValueDeserializer[F, V]]
  ): MkDeserializers[F, K, V] = {
    def _forKey = forKey
    def _forValue = forValue

    new MkDeserializers[F, K, V] {
      override def forKey: F[KeyDeserializer[F, K]] =
        _forKey

      override def forValue: F[ValueDeserializer[F, V]] =
        _forValue

      override def toString: String =
        "Deserializer.Record$" + System.identityHashCode(this)
    }
  }

  def lift[F[_], A](deserializer: => Deserializer[F, A])(
    implicit F: Applicative[F]
  ): MkDeserializers[F, A, A] =
    MkDeserializers.const(F.pure(deserializer))
}

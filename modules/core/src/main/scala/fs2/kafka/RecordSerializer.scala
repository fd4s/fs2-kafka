/*
 * Copyright 2018-2022 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.Resource

/**
  * Serializer which may vary depending on whether a record
  * key or value is being serialized, and which may require
  * a creation effect.
  */
sealed abstract class RecordSerializer[F[_], A] {
  def forKey: Resource[F, KeySerializer[F, A]]

  def forValue: Resource[F, ValueSerializer[F, A]]
}

object RecordSerializer {
  def apply[F[_], A](
    implicit serializer: RecordSerializer[F, A]
  ): RecordSerializer[F, A] =
    serializer

  def const[F[_], A](
    serializer: => Resource[F, Serializer[F, A]]
  ): RecordSerializer[F, A] =
    RecordSerializer.instance(
      forKey = serializer,
      forValue = serializer
    )

  def instance[F[_], A](
    forKey: => Resource[F, KeySerializer[F, A]],
    forValue: => Resource[F, ValueSerializer[F, A]]
  ): RecordSerializer[F, A] = {
    def _forKey = forKey
    def _forValue = forValue

    new RecordSerializer[F, A] {
      override def forKey: Resource[F, KeySerializer[F, A]] =
        _forKey

      override def forValue: Resource[F, ValueSerializer[F, A]] =
        _forValue

      override def toString: String =
        "Serializer.Record$" + System.identityHashCode(this)
    }
  }

  implicit def lift[F[_], A](implicit serializer: => Serializer[F, A]): RecordSerializer[F, A] =
    RecordSerializer.const(Resource.pure(serializer))
}

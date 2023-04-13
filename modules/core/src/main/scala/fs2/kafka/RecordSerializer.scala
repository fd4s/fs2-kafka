/*
 * Copyright 2018-2023 OVO Energy Limited
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
  def forKey: F[Serializer[F, A]]

  def forValue: F[Serializer[F, A]]

  /**
    * Returns a new [[RecordSerializer]] instance applying the mapping function to key and value serializers
    */
  final def transform[B](
    f: Serializer[F, A] => Serializer[F, B]
  )(implicit F: Functor[F]): RecordSerializer[F, B] =
    RecordSerializer.instance(
      forKey = forKey.map(f),
      forValue = forValue.map(f)
    )

  /**
    * Returns a new [[RecordSerializer]] instance that will serialize key and value `Some` values
    * using the specific [[Serializer]], and serialize `None` as `null`.
    *
    * See [[Serializer.option]] for more details.
    */
  final def option(implicit F: Functor[F]): RecordSerializer[F, Option[A]] =
    transform(_.option)
}

object RecordSerializer {
  def apply[F[_], A](
    implicit serializer: RecordSerializer[F, A]
  ): RecordSerializer[F, A] =
    serializer

  def const[F[_], A](
    serializer: => F[Serializer[F, A]]
  ): RecordSerializer[F, A] =
    RecordSerializer.instance(
      forKey = serializer,
      forValue = serializer
    )

  def instance[F[_], A](
    forKey: => F[Serializer[F, A]],
    forValue: => F[Serializer[F, A]]
  ): RecordSerializer[F, A] = {
    def _forKey: F[Serializer[F, A]] = forKey
    def _forValue: F[Serializer[F, A]] = forValue

    new RecordSerializer[F, A] {
      override def forKey: F[Serializer[F, A]] =
        _forKey

      override def forValue: F[Serializer[F, A]] =
        _forValue

      override def toString: String =
        "Serializer.Record$" + System.identityHashCode(this)
    }
  }

  def lift[F[_], A](serializer: => Serializer[F, A])(
    implicit F: Applicative[F]
  ): RecordSerializer[F, A] =
    RecordSerializer.const(F.pure(serializer))

  implicit def lift[F[_], A](
    implicit F: Applicative[F],
    serializer: Serializer[F, A]
  ): RecordSerializer[F, A] =
    RecordSerializer.lift(serializer)
}

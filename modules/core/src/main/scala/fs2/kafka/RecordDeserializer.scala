/*
 * Copyright 2018-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.syntax.all._
import cats.{Applicative, Functor}

/**
  * Deserializer which may vary depending on whether a record
  * key or value is being deserialized, and which may require
  * a creation effect.
  */
sealed abstract class RecordDeserializer[F[_], A] {
  def forKey: F[Deserializer[F, A]]

  def forValue: F[Deserializer[F, A]]

  /**
    * Returns a new [[RecordDeserializer]] instance that will catch deserialization
    * errors and return them as a value, allowing user code to handle them without
    * causing the consumer to fail.
    */
  final def attempt(implicit F: Functor[F]): RecordDeserializer[F, Either[Throwable, A]] =
    RecordDeserializer.instance(forKey.map(_.attempt), forValue.map(_.attempt))
}

object RecordDeserializer {
  def apply[F[_], A](
    implicit deserializer: RecordDeserializer[F, A]
  ): RecordDeserializer[F, A] =
    deserializer

  def const[F[_], A](
    deserializer: => F[Deserializer[F, A]]
  ): RecordDeserializer[F, A] =
    RecordDeserializer.instance(
      forKey = deserializer,
      forValue = deserializer
    )

  def instance[F[_], A](
    forKey: => F[Deserializer[F, A]],
    forValue: => F[Deserializer[F, A]]
  ): RecordDeserializer[F, A] = {
    def _forKey = forKey
    def _forValue = forValue

    new RecordDeserializer[F, A] {
      override def forKey: F[Deserializer[F, A]] =
        _forKey

      override def forValue: F[Deserializer[F, A]] =
        _forValue

      override def toString: String =
        "Deserializer.Record$" + System.identityHashCode(this)
    }
  }

  def lift[F[_], A](deserializer: => Deserializer[F, A])(
    implicit F: Applicative[F]
  ): RecordDeserializer[F, A] =
    RecordDeserializer.const(F.pure(deserializer))

  implicit def lift[F[_], A](
    implicit F: Applicative[F],
    deserializer: Deserializer[F, A]
  ): RecordDeserializer[F, A] =
    RecordDeserializer.lift(deserializer)
}

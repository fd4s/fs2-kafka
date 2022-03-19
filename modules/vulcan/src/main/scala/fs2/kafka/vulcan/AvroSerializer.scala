/*
 * Copyright 2018-2022 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan

import _root_.vulcan.Codec
import cats.effect.Sync
import fs2.kafka.{KeySerializer, Serializer, ValueSerializer}
import cats.effect.kernel.Resource

final class AvroSerializer[A] private[vulcan] (
  private val codec: Codec[A]
) extends AnyVal {

  /** Returns a `Serializer`, but will only correctly serialize either a key or a value
    * depending on `isKey` - therefore this method is private and can only be accessed
    * via `forKey` or `forValue`, which widen the return type to `KeySerializer` or
    * `ValueSerializer`.
    */
  private def createSerializer[F[_]](settings: AvroSettings[F], isKey: Boolean)(
    implicit F: Sync[F]
  ): Resource[F, Serializer[F, A]] =
    Resource
      .make(settings.createAvroSerializer(isKey)) { case (ser, _) => F.delay(ser.close()) }
      .map {
        case (serializer, _) =>
          Serializer.instance { (topic, _, a) =>
            F.defer {
              codec.encode(a) match {
                case Right(value) => F.pure(serializer.serialize(topic, value))
                case Left(error)  => F.raiseError(error.throwable)
              }
            }
          }
      }

  def forKey[F[_]: Sync](settings: AvroSettings[F]): Resource[F, KeySerializer[F, A]] =
    createSerializer(settings, isKey = true)

  def forValue[F[_]: Sync](settings: AvroSettings[F]): Resource[F, ValueSerializer[F, A]] =
    createSerializer(settings, isKey = false)

  override def toString: String =
    "AvroSerializer$" + System.identityHashCode(this)
}

object AvroSerializer {
  def apply[A](implicit codec: Codec[A]): AvroSerializer[A] =
    new AvroSerializer(codec)
}

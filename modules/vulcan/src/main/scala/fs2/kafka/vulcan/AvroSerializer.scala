/*
 * Copyright 2018-2020 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan

import _root_.vulcan.Codec
import cats.effect.Sync
import cats.implicits._
import fs2.kafka.{RecordSerializer, Serializer}

final class AvroSerializer[A] private[vulcan] (
  private val codec: Codec[A]
) extends AnyVal {
  def using[F[_]](
    settings: AvroSettings[F]
  )(implicit F: Sync[F]): RecordSerializer[F, A] =
    codec.schema match {
      case Right(schema) =>
        val createSerializer: Boolean => F[Serializer[F, A]] =
          settings.createAvroSerializer(_).map { serializer =>
            Serializer.instance { (topic, _, a) =>
              F.suspend {
                codec.encode(a, schema) match {
                  case Right(value) => F.pure(serializer.serialize(topic, value))
                  case Left(error)  => F.raiseError(error.throwable)
                }
              }
            }
          }

        RecordSerializer.instance(
          forKey = createSerializer(true),
          forValue = createSerializer(false)
        )

      case Left(error) =>
        RecordSerializer.const {
          F.raiseError(error.throwable)
        }
    }

  override def toString: String =
    "AvroSerializer$" + System.identityHashCode(this)
}

object AvroSerializer {
  def apply[A](implicit codec: Codec[A]): AvroSerializer[A] =
    new AvroSerializer(codec)
}

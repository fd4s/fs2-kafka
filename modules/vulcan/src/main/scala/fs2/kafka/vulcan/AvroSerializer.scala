/*
 * Copyright 2018-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan

import _root_.vulcan.Codec
import cats.effect.Sync
import cats.syntax.all._
import fs2.kafka.{RecordSerializer, Serializer}

final class AvroSerializer[A] private[vulcan] (
  private val codec: Codec[A]
) extends AnyVal {
  def using[F[_]](
    settings: AvroSettings[F]
  )(implicit F: Sync[F]): RecordSerializer[F, A] = {
    val createSerializer: Boolean => F[Serializer[F, A]] = isKey => {
      codec.schema match {
        case Left(e) => F.pure(Serializer.fail(e.throwable))
        case Right(writerSchema) =>
          settings.createAvroSerializer(isKey, Some(writerSchema)).map {
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
      }
    }

    RecordSerializer.instance(
      forKey = createSerializer(true),
      forValue = createSerializer(false)
    )
  }

  override def toString: String =
    "AvroSerializer$" + System.identityHashCode(this)
}

object AvroSerializer {
  def apply[A](implicit codec: Codec[A]): AvroSerializer[A] =
    new AvroSerializer(codec)
}

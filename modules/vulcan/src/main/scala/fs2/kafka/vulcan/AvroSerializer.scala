/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan

import _root_.vulcan.Codec
import cats.effect.Sync
import cats.implicits._
import fs2.kafka.{
  KeySerializer,
  MkKeySerializer,
  MkSerializers,
  MkValueSerializer,
  Serializer,
  ValueSerializer
}

final class AvroSerializer[A] private[vulcan] (
  private val codec: Codec[A]
) extends AnyVal {
  @deprecated(
    "use AvroSererializer[A].forKey, AvroSererializer[A].forValue, or AvroSererializers[A, A].using",
    "2.0.0"
  )
  def using[F[_]](
    settings: AvroSettings[F]
  )(implicit F: Sync[F]): MkSerializers[F, A, A] = {
    implicit val _codec: Codec[A] = codec
    AvroSerializers[A, A].using(settings)
  }

  def forKey[F[_]: Sync](settings: AvroSettings[F]): MkKeySerializer[F, A] =
    MkKeySerializer.instance(AvroSerializer.createKey(settings, codec))

  def forValue[F[_]: Sync](settings: AvroSettings[F]): MkValueSerializer[F, A] =
    MkValueSerializer.instance(AvroSerializer.createValue(settings, codec))

  override def toString: String =
    "AvroSerializer$" + System.identityHashCode(this)
}

object AvroSerializer {
  private[vulcan] def createKey[F[_], A](settings: AvroSettings[F], codec: Codec[A])(
    implicit F: Sync[F]
  ): F[KeySerializer[F, A]] =
    create(settings, codec, isKey = true).widen

  private[vulcan] def createValue[F[_], A](settings: AvroSettings[F], codec: Codec[A])(
    implicit F: Sync[F]
  ): F[ValueSerializer[F, A]] =
    create(settings, codec, isKey = false).widen

  private def create[F[_], A](settings: AvroSettings[F], codec: Codec[A], isKey: Boolean)(
    implicit F: Sync[F]
  ): F[Serializer[F, A]] =
    settings.createAvroSerializer(isKey).map {
      case (serializer, _) =>
        Serializer.instance { (topic, _, a) =>
          codec
            .encode(a)
            .leftMap(_.throwable)
            .liftTo[F]
            .flatMap { jAvro =>
              F.delay(serializer.serialize(topic, jAvro))
            }
        }
    }

  def apply[A](implicit codec: Codec[A]): AvroSerializer[A] =
    new AvroSerializer(codec)
}

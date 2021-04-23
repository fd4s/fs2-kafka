/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan

import _root_.vulcan.Codec
import cats.effect.Sync
import cats.implicits._
import fs2.kafka._
import io.confluent.kafka.schemaregistry.avro.AvroSchema

import java.nio.ByteBuffer

final class AvroDeserializer[A] private[vulcan] (
  private val codec: Codec[A]
) extends AnyVal {
  import AvroDeserializer._

  @deprecated(
    "use AvroDeserializer[A].forKey, AvroDeserializer[A].forValue, or AvroDeserializers[A, A].using",
    "2.0.0"
  )
  def using[F[_]](
    settings: AvroSettings[F]
  )(implicit F: Sync[F]): MkDeserializers[F, A, A] = {
    implicit val _codec: Codec[A] = codec
    AvroDeserializers[A, A].using(settings)
  }

  def forKey[F[_]](settings: AvroSettings[F])(implicit F: Sync[F]): MkKeyDeserializer[F, A] =
    MkKeyDeserializer.instance(createKey(settings, codec))

  def forValue[F[_]](settings: AvroSettings[F])(implicit F: Sync[F]): MkValueDeserializer[F, A] =
    MkValueDeserializer.instance(createValue(settings, codec))

  override def toString: String =
    "AvroDeserializer$" + System.identityHashCode(this)
}

object AvroDeserializer {
  def apply[A](implicit codec: Codec[A]): AvroDeserializer[A] =
    new AvroDeserializer(codec)

  private[vulcan] def createKey[F[_], A](settings: AvroSettings[F], codec: Codec[A])(
    implicit F: Sync[F]
  ): F[KeyDeserializer[F, A]] =
    create(settings, codec, isKey = true).widen

  private[vulcan] def createValue[F[_], A](settings: AvroSettings[F], codec: Codec[A])(
    implicit F: Sync[F]
  ): F[ValueDeserializer[F, A]] =
    create(settings, codec, isKey = false).widen

  private def create[F[_], A](settings: AvroSettings[F], codec: Codec[A], isKey: Boolean)(
    implicit F: Sync[F]
  ): F[Deserializer[F, A]] =
    codec.schema.leftMap(_.throwable).liftTo[F].flatMap { schema =>
      settings.createAvroDeserializer(isKey).map {
        case (deserializer, schemaRegistryClient) =>
          Deserializer.instance { (topic, _, bytes) =>
            F.defer {
              val writerSchemaId =
                ByteBuffer.wrap(bytes).getInt(1) // skip magic byte

              val writerSchema = {
                val schema = schemaRegistryClient.getSchemaById(writerSchemaId)
                if (schema.isInstanceOf[AvroSchema])
                  schema.asInstanceOf[AvroSchema].rawSchema()
                else
                  null
              }

              codec
                .decode(deserializer.deserialize(topic, bytes, schema), writerSchema)
                .leftMap(_.throwable)
                .liftTo[F]
            }
          }
      }
    }
}

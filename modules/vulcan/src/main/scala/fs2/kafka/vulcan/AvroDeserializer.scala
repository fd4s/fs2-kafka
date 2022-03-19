/*
 * Copyright 2018-2022 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan

import _root_.vulcan.Codec
import cats.effect.Sync
import cats.effect.Resource
import fs2.kafka.{Deserializer, KeyDeserializer, ValueDeserializer}
import io.confluent.kafka.schemaregistry.avro.AvroSchema

import java.nio.ByteBuffer

final class AvroDeserializer[A] private[vulcan] (
  private val codec: Codec[A]
) extends AnyVal {
  private def createDeserializer[F[_]](
    settings: AvroSettings[F],
    isKey: Boolean
  )(implicit F: Sync[F]): Resource[F, Deserializer[F, A]] =
    codec.schema match {
      case Right(schema) =>
        Resource
          .make(settings.createAvroDeserializer(isKey)) {
            case (deserializer, _) => F.delay(deserializer.close())
          }
          .map {
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

                  codec.decode(deserializer.deserialize(topic, bytes, schema), writerSchema) match {
                    case Right(a)    => F.pure(a)
                    case Left(error) => F.raiseError(error.throwable)
                  }
                }
              }
          }

      case Left(error) =>
        Resource.raiseError(error.throwable)
    }

  def forKey[F[_]: Sync](settings: AvroSettings[F]): Resource[F, KeyDeserializer[F, A]] =
    createDeserializer(settings, isKey = true)

  def forValue[F[_]: Sync](settings: AvroSettings[F]): Resource[F, ValueDeserializer[F, A]] =
    createDeserializer(settings, isKey = true)

  override def toString: String =
    "AvroDeserializer$" + System.identityHashCode(this)
}

object AvroDeserializer {
  def apply[A](implicit codec: Codec[A]): AvroDeserializer[A] =
    new AvroDeserializer(codec)
}

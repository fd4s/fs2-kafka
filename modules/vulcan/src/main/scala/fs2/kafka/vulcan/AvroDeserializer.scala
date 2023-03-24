/*
 * Copyright 2018-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan

import _root_.vulcan.Codec
import cats.effect.Sync
import cats.syntax.all._
import fs2.kafka.{Deserializer, RecordDeserializer}
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import java.nio.ByteBuffer

final class AvroDeserializer[A] private[vulcan] (
  private val codec: Codec[A]
) extends AnyVal {
  def using[F[_]](
    settings: AvroSettings[F]
  )(implicit F: Sync[F]): RecordDeserializer[F, A] =
    codec.schema match {
      case Right(schema) =>
        val createDeserializer: Boolean => F[Deserializer[F, A]] =
          settings.createAvroDeserializer(_).map {
            case (deserializer, schemaRegistryClient) =>
              Deserializer.instance { (topic, _, bytes) =>
                F.defer {
                  if (bytes == null || bytes.length == 0) {
                    F.raiseError(
                      new IllegalArgumentException(
                        s"Invalid Avro record: bytes is null or empty"
                      )
                    )

                  } else {
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
                      .decode(deserializer.deserialize(topic, bytes, schema), writerSchema) match {
                      case Right(a)    => F.pure(a)
                      case Left(error) => F.raiseError(error.throwable)
                    }
                  }
                }
              }
          }

        RecordDeserializer.instance(
          forKey = createDeserializer(true),
          forValue = createDeserializer(false)
        )

      case Left(error) =>
        RecordDeserializer.const {
          F.raiseError(error.throwable)
        }
    }

  override def toString: String =
    "AvroDeserializer$" + System.identityHashCode(this)
}

object AvroDeserializer {
  def apply[A](implicit codec: Codec[A]): AvroDeserializer[A] =
    new AvroDeserializer(codec)
}

/*
 * Copyright 2018-2019 OVO Energy Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fs2.kafka.vulcan

import java.nio.ByteBuffer

import _root_.vulcan.Codec
import cats.effect.Sync
import cats.implicits._
import fs2.kafka.Deserializer

final class AvroDeserializer[A] private[vulcan] (
  private val codec: Codec[A]
) extends AnyVal {
  def using[F[_]](
    settings: AvroSettings[F]
  )(implicit F: Sync[F]): Deserializer.Record[F, A] =
    codec.schema match {
      case Right(schema) =>
        val createDeserializer: Boolean => F[Deserializer[F, A]] = isKey =>
          settings.schemaRegistryClient.flatMap { schemaRegistryClient =>
            settings.createAvroDeserializer(isKey).map { deserializer =>
              Deserializer.instance { (topic, _, bytes) =>
                F.suspend {
                  val writerSchemaId = ByteBuffer.wrap(bytes).getInt(1) // skip magic byte
                  val writerSchema = schemaRegistryClient.getById(writerSchemaId)

                  codec.decode(deserializer.deserialize(topic, bytes, schema), writerSchema) match {
                    case Right(a)    => F.pure(a)
                    case Left(error) => F.raiseError(error.throwable)
                  }
                }
              }
            }
          }

        Deserializer.Record.instance(
          forKey = createDeserializer(true),
          forValue = createDeserializer(false)
        )

      case Left(error) =>
        Deserializer.Record.const {
          F.raiseError(error.throwable)
        }
    }

  override def toString: String =
    "AvroDeserializer$" + System.identityHashCode(this)
}

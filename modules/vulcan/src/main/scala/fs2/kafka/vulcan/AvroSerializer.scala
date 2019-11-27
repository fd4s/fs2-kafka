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

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

package fs2.kafka

import _root_.vulcan.Codec
import cats.effect.Sync
import cats.implicits._

package object vulcan {

  /** Alias for `io.confluent.kafka.schemaregistry.client.SchemaRegistryClient`. */
  type SchemaRegistryClient =
    io.confluent.kafka.schemaregistry.client.SchemaRegistryClient

  /** Alias for `io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient`. */
  type CachedSchemaRegistryClient =
    io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient

  /** Alias for `io.confluent.kafka.serializers.KafkaAvroDeserializer`. */
  type KafkaAvroDeserializer =
    io.confluent.kafka.serializers.KafkaAvroDeserializer

  /** Alias for `io.confluent.kafka.serializers.KafkaAvroSerializer`. */
  type KafkaAvroSerializer =
    io.confluent.kafka.serializers.KafkaAvroSerializer

  /**
    * Creates an Avro `Deserializer` using specified [[AvroSettings]].
    */
  def avroDeserializer[F[_], A](
    settings: AvroSettings[F, A]
  )(
    implicit F: Sync[F],
    codec: Codec[A]
  ): F[Deserializer[F, A]] =
    codec.schema match {
      case Right(schema) =>
        settings.createAvroDeserializer.map { deserializer =>
          Deserializer.instance { (topic, _, bytes) =>
            F.suspend {
              codec.decode(deserializer.deserialize(topic, bytes, schema), schema) match {
                case Right(a)    => F.pure(a)
                case Left(error) => F.raiseError(error.throwable)
              }
            }
          }
        }

      case Left(error) =>
        F.raiseError(error.throwable)
    }

  implicit def avroDeserializer[F[_], A](
    implicit F: Sync[F],
    codec: Codec[A],
    settings: AvroSettings[F, A]
  ): F[Deserializer[F, A]] =
    avroDeserializer(settings)

  /**
    * Creates an Avro `Serializer` using specified [[AvroSettings]].
    */
  def avroSerializer[F[_], A](
    settings: AvroSettings[F, A]
  )(
    implicit F: Sync[F],
    codec: Codec[A]
  ): F[Serializer[F, A]] =
    codec.schema match {
      case Right(schema) =>
        settings.createAvroSerializer.map { serializer =>
          Serializer.instance { (topic, _, a) =>
            F.suspend {
              codec.encode(a, schema) match {
                case Right(value) => F.pure(serializer.serialize(topic, value))
                case Left(error)  => F.raiseError(error.throwable)
              }
            }
          }
        }

      case Left(error) =>
        F.raiseError(error.throwable)
    }

  implicit def avroSerializer[F[_], A](
    implicit F: Sync[F],
    codec: Codec[A],
    settings: AvroSettings[F, A]
  ): F[Serializer[F, A]] =
    avroSerializer(settings)
}

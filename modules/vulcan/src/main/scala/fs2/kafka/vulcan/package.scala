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

  def avroDeserializer[A](implicit codec: Codec[A]): AvroDeserializer[A] =
    new AvroDeserializer(codec)

  def avroSerializer[A](implicit codec: Codec[A]): AvroSerializer[A] =
    new AvroSerializer(codec)
}

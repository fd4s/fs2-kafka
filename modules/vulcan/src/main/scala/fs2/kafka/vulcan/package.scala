/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

package object vulcan {

  /** Alias for `io.confluent.kafka.schemaregistry.client.SchemaRegistryClient`. */
  type JavaSchemaRegistryClient =
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
}

package fs2.kafka.schemaregistry

package object client {

  /** Alias for `io.confluent.kafka.schemaregistry.client.SchemaMetadata`. */
  type SchemaMetadata =
    io.confluent.kafka.schemaregistry.client.SchemaMetadata

  /** Alias for `io.confluent.kafka.schemaregistry.ParsedSchema`. */
  type ParsedSchema =
    io.confluent.kafka.schemaregistry.ParsedSchema

  /** Alias for `io.confluent.kafka.schemaregistry.client.SchemaRegistryClient`. */
  type JSchemaRegistryClient =
    io.confluent.kafka.schemaregistry.client.SchemaRegistryClient

  /** Alias for `io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient`. */
  type JCachedSchemaRegistryClient =
    io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
}

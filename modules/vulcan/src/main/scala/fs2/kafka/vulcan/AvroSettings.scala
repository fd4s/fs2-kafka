/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan

import cats.effect.Sync
import cats.implicits._
import fs2.kafka.internal.converters.collection._
import fs2.kafka.internal.syntax._
import fs2.kafka.security.KafkaCredentialStore

/**
  * Describes how to create a `KafkaAvroDeserializer` and a
  * `KafkaAvroSerializer` and which settings should be used.
  * Settings are tailored for the Confluent Kafka Avro
  * serializers and deserializers.
  *
  * Use `AvroSettings.apply` to create an instance.
  */
sealed abstract class AvroSettings[F[_]] {

  /**
    * The `SchemaRegistryClient` to use for the serializers
    * and deserializers created from this [[AvroSettings]].
    */
  def schemaRegistryClient: F[SchemaRegistryClient]

  /**
    * Creates a new `AvroSettings` instance with the specified
    * setting for whether serializers should register schemas
    * automatically or not.
    *
    * The default value is `true`.
    */
  def withAutoRegisterSchemas(autoRegisterSchemas: Boolean): AvroSettings[F]

  /**
    * Creates a new `AvroSettings` instance with the specified
    * key subject name strategy. This is the class name of the
    * strategy which should be used.
    *
    * The default value is `io.confluent.kafka.serializers.subject.TopicNameStrategy`.
    */
  def withKeySubjectNameStrategy(keySubjectNameStrategy: String): AvroSettings[F]

  /**
    * Creates a new `AvroSettings` instance with the specified
    * value subject name strategy. This is the class name of
    * the strategy which should be used.
    *
    * The default value is `io.confluent.kafka.serializers.subject.TopicNameStrategy`.
    */
  def withValueSubjectNameStrategy(valueSubjectNameStrategy: String): AvroSettings[F]

  /**
    * Properties provided when creating a Confluent Kafka Avro
    * serializer or deserializer. Functions in [[AvroSettings]]
    * add properties here as necessary.
    */
  def properties: Map[String, String]

  /**
    * Creates a new [[AvroSettings]] instance including a
    * property with the specified key and value.
    */
  def withProperty(key: String, value: String): AvroSettings[F]

  /**
    * Creates a new [[AvroSettings]] instance including
    * properties with the specified keys and values.
    */
  def withProperties(properties: (String, String)*): AvroSettings[F]

  /**
    * Creates a new [[AvroSettings]] instance including
    * properties with the specified keys and values.
    */
  def withProperties(properties: Map[String, String]): AvroSettings[F]

  /**
    * Creates a new `KafkaAvroDeserializer` using the settings
    * contained within this [[AvroSettings]] instance, and the
    * specified `isKey` flag, denoting whether a record key or
    * value is being deserialized.
    */
  def createAvroDeserializer(isKey: Boolean): F[(KafkaAvroDeserializer, SchemaRegistryClient)]

  /**
    * Creates a new `KafkaAvroSerializer` using the settings
    * contained within this [[AvroSettings]] instance, and the
    * specified `isKey` flag, denoting whether a record key or
    * value is being serialized.
    */
  def createAvroSerializer(isKey: Boolean): F[(KafkaAvroSerializer, SchemaRegistryClient)]

  /**
    * Creates a new [[AvroSettings]] instance with the specified
    * function for creating `KafkaAvroDeserializer`s from settings.
    * The arguments are [[schemaRegistryClient]], `isKey`, and
    * [[properties]].
    */
  def withCreateAvroDeserializer(
    // format: off
    createAvroDeserializerWith: (F[SchemaRegistryClient], Boolean, Map[String, String]) => F[(KafkaAvroDeserializer, SchemaRegistryClient)]
    // format: on
  ): AvroSettings[F]

  /**
    * Creates a new [[AvroSettings]] instance with the specified
    * function for creating `KafkaAvroSerializer`s from settings.
    * The arguments are [[schemaRegistryClient]], `isKey`, and
    * [[properties]].
    */
  def withCreateAvroSerializer(
    // format: off
    createAvroSerializerWith: (F[SchemaRegistryClient], Boolean, Map[String, String]) => F[(KafkaAvroSerializer, SchemaRegistryClient)]
    // format: on
  ): AvroSettings[F]

  /**
    * Includes the credentials properties from the provided [[KafkaCredentialStore]]
    */
  def withCredentials(credentialsStore: KafkaCredentialStore): AvroSettings[F] =
    withProperties(credentialsStore.properties)
}

object AvroSettings {
  private[this] final case class AvroSettingsImpl[F[_]](
    override val schemaRegistryClient: F[SchemaRegistryClient],
    override val properties: Map[String, String],
    // format: off
    val createAvroDeserializerWith: (F[SchemaRegistryClient], Boolean, Map[String, String]) => F[(KafkaAvroDeserializer, SchemaRegistryClient)],
    val createAvroSerializerWith: (F[SchemaRegistryClient], Boolean, Map[String, String]) => F[(KafkaAvroSerializer, SchemaRegistryClient)]
    // format: on
  ) extends AvroSettings[F] {
    override def withAutoRegisterSchemas(autoRegisterSchemas: Boolean): AvroSettings[F] =
      withProperty("auto.register.schemas", autoRegisterSchemas.toString)

    override def withKeySubjectNameStrategy(
      keySubjectNameStrategy: String
    ): AvroSettings[F] =
      withProperty("key.subject.name.strategy", keySubjectNameStrategy)

    override def withValueSubjectNameStrategy(
      valueSubjectNameStrategy: String
    ): AvroSettings[F] =
      withProperty("value.subject.name.strategy", valueSubjectNameStrategy)

    override def withProperty(key: String, value: String): AvroSettings[F] =
      copy(properties = properties.updated(key, value))

    override def withProperties(properties: (String, String)*): AvroSettings[F] =
      copy(properties = this.properties ++ properties.toMap)

    override def withProperties(properties: Map[String, String]): AvroSettings[F] =
      copy(properties = this.properties ++ properties)

    override def createAvroDeserializer(
      isKey: Boolean
    ): F[(KafkaAvroDeserializer, SchemaRegistryClient)] =
      createAvroDeserializerWith(schemaRegistryClient, isKey, properties)

    override def createAvroSerializer(
      isKey: Boolean
    ): F[(KafkaAvroSerializer, SchemaRegistryClient)] =
      createAvroSerializerWith(schemaRegistryClient, isKey, properties)

    override def withCreateAvroDeserializer(
      // format: off
      createAvroDeserializerWith: (F[SchemaRegistryClient], Boolean, Map[String, String]) => F[(KafkaAvroDeserializer, SchemaRegistryClient)]
      // format: on
    ): AvroSettings[F] =
      copy(createAvroDeserializerWith = createAvroDeserializerWith)

    override def withCreateAvroSerializer(
      // format: off
      createAvroSerializerWith: (F[SchemaRegistryClient], Boolean, Map[String, String]) => F[(KafkaAvroSerializer, SchemaRegistryClient)]
      // format: on
    ): AvroSettings[F] =
      copy(createAvroSerializerWith = createAvroSerializerWith)

    override def toString: String =
      "AvroSettings$" + System.identityHashCode(this)
  }

  private[this] def withDefaults(properties: Map[String, String]) =
    properties.updatedIfAbsent("schema.registry.url", "").asJava

  private[this] def create[F[_]](
    schemaRegistryClient: F[SchemaRegistryClient]
  )(implicit F: Sync[F]): AvroSettings[F] =
    AvroSettingsImpl(
      schemaRegistryClient = schemaRegistryClient,
      properties = Map.empty,
      createAvroDeserializerWith = (schemaRegistryClient, isKey, properties) =>
        schemaRegistryClient.flatMap { schemaRegistryClient =>
          F.delay {
            val deserializer = new KafkaAvroDeserializer(schemaRegistryClient)
            deserializer.configure(withDefaults(properties), isKey)
            (deserializer, schemaRegistryClient)
          }
        },
      createAvroSerializerWith = (schemaRegistryClient, isKey, properties) =>
        schemaRegistryClient.flatMap { schemaRegistryClient =>
          F.delay {
            val serializer = new KafkaAvroSerializer(schemaRegistryClient)
            serializer.configure(withDefaults(properties), isKey)
            (serializer, schemaRegistryClient)
          }
        }
    )

  def apply[F[_]](
    schemaRegistryClientSettings: SchemaRegistryClientSettings[F]
  )(implicit F: Sync[F]): AvroSettings[F] =
    create(schemaRegistryClientSettings.createSchemaRegistryClient)

  def apply[F[_]](
    schemaRegistryClient: SchemaRegistryClient
  )(implicit F: Sync[F]): AvroSettings[F] =
    create(F.pure(schemaRegistryClient))
}

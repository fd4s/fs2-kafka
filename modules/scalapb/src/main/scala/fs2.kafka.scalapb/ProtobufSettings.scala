/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.scalapb

import cats.effect.Sync
import cats.implicits._
import com.google.protobuf.Message
import fs2.kafka.internal.converters.collection._
import fs2.kafka.internal.syntax._
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig

import scala.reflect.ClassTag

/**
  * Describes how to create a `KafkaProtobufDeserializer` and a
  * `KafkaProtobufSerializer` and which settings should be used.
  * Settings are tailored for the Confluent Kafka Protobuf
  * serializers and deserializers.
  *
  * Use `ProtobufSettings.apply` to create an instance.
  */
sealed abstract class ProtobufSettings[F[_], JavaProto <: Message] {

  /**
    * The `SchemaRegistryClient` to use for the serializers
    * and deserializers created from this [[ProtobufSettings]].
    */
  def schemaRegistryClient: F[SchemaRegistryClient]

  /**
    * Creates a new `ProtobufSettings` instance with the specified
    * setting for whether serializers should register schemas
    * automatically or not.
    *
    * The default value is `true`.
    */
  def withAutoRegisterSchemas(autoRegisterSchemas: Boolean): ProtobufSettings[F, JavaProto]

  /**
    * Creates a new `ProtobufSettings` instance with the specified
    * key subject name strategy. This is the class name of the
    * strategy which should be used.
    *
    * The default value is `io.confluent.kafka.serializers.subject.TopicNameStrategy`.
    */
  def withKeySubjectNameStrategy(keySubjectNameStrategy: String): ProtobufSettings[F, JavaProto]

  /**
    * Creates a new `ProtobufSettings` instance with the specified
    * value subject name strategy. This is the class name of
    * the strategy which should be used.
    *
    * The default value is `io.confluent.kafka.serializers.subject.TopicNameStrategy`.
    */
  def withValueSubjectNameStrategy(valueSubjectNameStrategy: String): ProtobufSettings[F, JavaProto]

  /**
    * Properties provided when creating a Confluent Kafka Protobuf
    * serializer or deserializer. Functions in [[ProtobufSettings]]
    * add properties here as necessary.
    */
  def properties: Map[String, String]

  /**
    * Creates a new [[ProtobufSettings]] instance including a
    * property with the specified key and value.
    */
  def withProperty(key: String, value: String): ProtobufSettings[F, JavaProto]

  /**
    * Creates a new [[ProtobufSettings]] instance including
    * properties with the specified keys and values.
    */
  def withProperties(properties: (String, String)*): ProtobufSettings[F, JavaProto]

  /**
    * Creates a new [[ProtobufSettings]] instance including
    * properties with the specified keys and values.
    */
  def withProperties(properties: Map[String, String]): ProtobufSettings[F, JavaProto]

  /**
    * Creates a new `KafkaProtobufDeserializer` using the settings
    * contained within this [[ProtobufSettings]] instance, and the
    * specified `isKey` flag, denoting whether a record key or
    * value is being deserialized.
    */
  def createProtobufDeserializer(isKey: Boolean): F[KafkaProtobufDeserializer[JavaProto]]

  /**
    * Creates a new `KafkaProtobufSerializer` using the settings
    * contained within this [[ProtobufSettings]] instance, and the
    * specified `isKey` flag, denoting whether a record key or
    * value is being serialized.
    */
  def createProtobufSerializer(isKey: Boolean): F[KafkaProtobufSerializer[JavaProto]]


  /**
    * Creates a new [[ProtobufSettings]] instance with the specified
    * function for creating `KafkaProtobufDeserializer`s from settings.
    * The arguments are [[schemaRegistryClient]], `isKey`, and
    * [[properties]].
    */
  def withCreateProtobufDeserializer(
                                  // format: off
                                  createProtobufDeserializerWith: (F[SchemaRegistryClient], Boolean, Map[String, String]) => F[KafkaProtobufDeserializer[JavaProto]]
                                  // format: on
                                ): ProtobufSettings[F, JavaProto]

  /**
    * Creates a new [[ProtobufSettings]] instance with the specified
    * function for creating `KafkaProtobufSerializer`s from settings.
    * The arguments are [[schemaRegistryClient]], `isKey`, and
    * [[properties]].
    */
  def withCreateProtobufSerializer(
                                // format: off
                                createProtobufSerializerWith: (F[SchemaRegistryClient], Boolean, Map[String, String]) => F[KafkaProtobufSerializer[JavaProto]]
                                // format: on
                              ): ProtobufSettings[F, JavaProto]


}

object ProtobufSettings {

  private[this] final case class ProtobufSettingsImpl[F[_], JavaProto <: Message](
                                                                        override val schemaRegistryClient: F[SchemaRegistryClient],
                                                                        override val properties: Map[String, String],
                                                                        val createProtobufDeserializerWith: (F[SchemaRegistryClient], Boolean, Map[String, String]) => F[KafkaProtobufDeserializer[JavaProto]],
                                                                        val createProtobufSerializerWith: (F[SchemaRegistryClient], Boolean, Map[String, String]) => F[KafkaProtobufSerializer[JavaProto]]
  ) extends ProtobufSettings[F, JavaProto] {
    override def withAutoRegisterSchemas(
      autoRegisterSchemas: Boolean
    ): ProtobufSettings[F, JavaProto] =
      withProperty("auto.register.schemas", autoRegisterSchemas.toString)

    override def withKeySubjectNameStrategy(
      keySubjectNameStrategy: String
    ): ProtobufSettings[F, JavaProto] =
      withProperty("key.subject.name.strategy", keySubjectNameStrategy)

    override def withValueSubjectNameStrategy(
      valueSubjectNameStrategy: String
    ): ProtobufSettings[F, JavaProto] =
      withProperty("value.subject.name.strategy", valueSubjectNameStrategy)

    override def withProperty(key: String, value: String): ProtobufSettings[F, JavaProto] =
      copy(properties = properties.updated(key, value))

    override def withProperties(properties: (String, String)*): ProtobufSettings[F, JavaProto] =
      copy(properties = this.properties ++ properties.toMap)

    override def withProperties(properties: Map[String, String]): ProtobufSettings[F, JavaProto] =
      copy(properties = this.properties ++ properties)

    override def createProtobufDeserializer(
      isKey: Boolean
    ): F[KafkaProtobufDeserializer[JavaProto]] =
      createProtobufDeserializerWith(schemaRegistryClient, isKey, properties)

    override def createProtobufSerializer(
      isKey: Boolean
    ): F[KafkaProtobufSerializer[JavaProto]] =
      createProtobufSerializerWith(schemaRegistryClient, isKey, properties)


    override def withCreateProtobufDeserializer(createProtobufDeserializerWith: (F[SchemaRegistryClient], Boolean, Map[String, String]) => F[KafkaProtobufDeserializer[JavaProto]]): ProtobufSettings[F, JavaProto] =
      copy(createProtobufDeserializerWith = createProtobufDeserializerWith)

    override def withCreateProtobufSerializer(createProtobufSerializerWith: (F[SchemaRegistryClient], Boolean, Map[String, String]) => F[KafkaProtobufSerializer[JavaProto]]): ProtobufSettings[F, JavaProto] =
      copy(createProtobufSerializerWith = createProtobufSerializerWith)

    override def toString: String =
      "ProtobufSettings$" + System.identityHashCode(this)
  }

  private[this] def withDefaults(properties: Map[String, String]) =
    properties.updatedIfAbsent("schema.registry.url", "").asJava

  private[this] def create[F[_], JavaProto <: Message](
    schemaRegistryClient: F[SchemaRegistryClient]
  )(implicit F: Sync[F], classTag: ClassTag[JavaProto]): ProtobufSettings[F, JavaProto] = {
    val properties = Map(
      KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE -> classTag.runtimeClass.getName
    )
    ProtobufSettingsImpl(
      schemaRegistryClient = schemaRegistryClient,
      properties = properties,
      createProtobufDeserializerWith = (schemaRegistryClient, isKey, properties) =>
        schemaRegistryClient.flatMap { schemaRegistryClient =>
          F.delay {
            val deserializer = new KafkaProtobufDeserializer[JavaProto](schemaRegistryClient)
            deserializer.configure(withDefaults(properties), isKey)
            deserializer
          }
        },
        createProtobufSerializerWith = (schemaRegistryClient, isKey, properties) =>
          schemaRegistryClient.flatMap { schemaRegistryClient =>
            F.delay {
              val serializer = new KafkaProtobufSerializer[JavaProto](schemaRegistryClient)
              serializer.configure(withDefaults(properties), isKey)
              serializer
            }
          }
    )
  }

  def apply[F[_], JavaProto <: Message](
    schemaRegistryClientSettings: SchemaRegistryClientSettings[F]
  )(implicit F: Sync[F], classTag: ClassTag[JavaProto]): ProtobufSettings[F, JavaProto] =
    create(schemaRegistryClientSettings.createSchemaRegistryClient)

  def apply[F[_], JavaProto <: Message](
    schemaRegistryClient: SchemaRegistryClient
  )(implicit F: Sync[F], classTag: ClassTag[JavaProto]): ProtobufSettings[F, JavaProto] =
    create(F.pure(schemaRegistryClient))
}

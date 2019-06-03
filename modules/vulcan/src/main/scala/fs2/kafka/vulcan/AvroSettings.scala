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

import cats.effect.Sync
import cats.implicits._
import cats.Show
import fs2.kafka.internal.converters.collection._
import fs2.kafka.internal.syntax._

/**
  * Describes how to create a `KafkaAvroDeserializer` and a
  * `KafkaAvroSerializer` and which settings should be used.
  * Settings are tailored for the Confluent Kafka Avro
  * serializers and deserializers.<br>
  * <br>
  * Use `AvroSettings#apply` to create an instance.
  */
sealed abstract class AvroSettings[F[_], A] {

  /**
    * The `SchemaRegistryClient` to use for the serializers
    * and deserializers created from this [[AvroSettings]].
    */
  def schemaRegistryClient: F[SchemaRegistryClient]

  /**
    * Creates a new `AvroSettings` instance with the specified
    * setting for whether serializers should register schemas
    * automatically or not.<br>
    * <br>
    * The default value is `true`.
    */
  def withAutoRegisterSchemas(autoRegisterSchemas: Boolean): AvroSettings[F, A]

  /**
    * Creates a new `AvroSettings` instance with the specified
    * key subject name strategy. This is the class name of the
    * strategy which should be used.<br>
    * <br>
    * The default value is `io.confluent.kafka.serializers.subject.TopicNameStrategy`.
    */
  def withKeySubjectNameStrategy(keySubjectNameStrategy: String): AvroSettings[F, A]

  /**
    * Creates a new `AvroSettings` instance with the specified
    * value subject name strategy. This is the class name of
    * the strategy which should be used.<br>
    * <br>
    * The default value is `io.confluent.kafka.serializers.subject.TopicNameStrategy`.
    */
  def withValueSubjectNameStrategy(valueSubjectNameStrategy: String): AvroSettings[F, A]

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
  def withProperty(key: String, value: String): AvroSettings[F, A]

  /**
    * Creates a new [[AvroSettings]] instance including
    * properties with the specified keys and values.
    */
  def withProperties(properties: (String, String)*): AvroSettings[F, A]

  /**
    * Creates a new [[AvroSettings]] instance including
    * properties with the specified keys and values.
    */
  def withProperties(properties: Map[String, String]): AvroSettings[F, A]

  /**
    * Returns `true` if the settings are for a record key;
    * false if the settings are for a record value.<br>
    * <br>
    * The default value is `false`.
    */
  def isKey: Boolean

  /**
    * Creates a new `AvroSettings` instance with the
    * specified setting for [[isKey]].
    */
  def withIsKey(isKey: Boolean): AvroSettings[F, A]

  /**
    * Creates a new `KafkaAvroDeserializer` using the settings
    * contained within this [[AvroSettings]] instance.
    */
  def createAvroDeserializer: F[KafkaAvroDeserializer]

  /**
    * Creates a new `KafkaAvroSerializer` using the settings
    * contained within this [[AvroSettings]] instance.
    */
  def createAvroSerializer: F[KafkaAvroSerializer]

  /**
    * Creates a new [[AvroSettings]] instance with the specified
    * function for creating new `KafkaAvroDeserializer`s from
    * settings. The arguments are [[schemaRegistryClient]],
    * [[isKey]], and [[properties]].
    */
  def withCreateAvroDeserializer(
    // format: off
    createAvroDeserializerWith: (F[SchemaRegistryClient], Boolean, Map[String, String]) => F[KafkaAvroDeserializer]
    // format: on
  ): AvroSettings[F, A]

  /**
    * Creates a new [[AvroSettings]] instance with the specified
    * function for creating new `KafkaAvroSerializer`s from
    * settings. The arguments are [[schemaRegistryClient]],
    * [[isKey]], and [[properties]].
    */
  def withCreateAvroSerializer(
    // format: off
    createAvroSerializerWith: (F[SchemaRegistryClient], Boolean, Map[String, String]) => F[KafkaAvroSerializer]
    // format: on
  ): AvroSettings[F, A]
}

object AvroSettings {
  private[this] final case class AvroSettingsImpl[F[_], A](
    override val schemaRegistryClient: F[SchemaRegistryClient],
    override val properties: Map[String, String],
    override val isKey: Boolean,
    // format: off
    val createAvroDeserializerWith: (F[SchemaRegistryClient], Boolean, Map[String, String]) => F[KafkaAvroDeserializer],
    val createAvroSerializerWith: (F[SchemaRegistryClient], Boolean, Map[String, String]) => F[KafkaAvroSerializer]
    // format: on
  ) extends AvroSettings[F, A] {
    override def withAutoRegisterSchemas(autoRegisterSchemas: Boolean): AvroSettings[F, A] =
      withProperty("auto.register.schemas", autoRegisterSchemas.toString)

    override def withKeySubjectNameStrategy(
      keySubjectNameStrategy: String
    ): AvroSettings[F, A] =
      withProperty("key.subject.name.strategy", keySubjectNameStrategy)

    override def withValueSubjectNameStrategy(
      valueSubjectNameStrategy: String
    ): AvroSettings[F, A] =
      withProperty("value.subject.name.strategy", valueSubjectNameStrategy)

    override def withProperty(key: String, value: String): AvroSettings[F, A] =
      copy(properties = properties.updated(key, value))

    override def withProperties(properties: (String, String)*): AvroSettings[F, A] =
      copy(properties = this.properties ++ properties.toMap)

    override def withProperties(properties: Map[String, String]): AvroSettings[F, A] =
      copy(properties = this.properties ++ properties)

    override def withIsKey(isKey: Boolean): AvroSettings[F, A] =
      copy(isKey = isKey)

    override def createAvroDeserializer: F[KafkaAvroDeserializer] =
      createAvroDeserializerWith(schemaRegistryClient, isKey, properties)

    override def createAvroSerializer: F[KafkaAvroSerializer] =
      createAvroSerializerWith(schemaRegistryClient, isKey, properties)

    def withCreateAvroDeserializer(
      // format: off
      createAvroDeserializerWith: (F[SchemaRegistryClient], Boolean, Map[String, String]) => F[KafkaAvroDeserializer]
      // format: on
    ): AvroSettings[F, A] =
      copy(createAvroDeserializerWith = createAvroDeserializerWith)

    def withCreateAvroSerializer(
      // format: off
      createAvroSerializerWith: (F[SchemaRegistryClient], Boolean, Map[String, String]) => F[KafkaAvroSerializer]
      // format: on
    ): AvroSettings[F, A] =
      copy(createAvroSerializerWith = createAvroSerializerWith)

    override def toString: String =
      s"AvroSettings(isKey = $isKey)"
  }

  private[this] def withDefaults[F[_], A](properties: Map[String, String]) =
    properties.updatedIfAbsent("schema.registry.url", "").asJava

  private[this] def create[F[_], A](
    schemaRegistryClient: F[SchemaRegistryClient]
  )(implicit F: Sync[F]): AvroSettings[F, A] =
    AvroSettingsImpl(
      schemaRegistryClient = schemaRegistryClient,
      properties = Map.empty,
      isKey = false,
      createAvroDeserializerWith = (schemaRegistryClient, isKey, properties) =>
        schemaRegistryClient.flatMap { schemaRegistryClient =>
          F.delay {
            val deserializer = new KafkaAvroDeserializer(schemaRegistryClient)
            deserializer.configure(withDefaults(properties), isKey)
            deserializer
          }
        },
      createAvroSerializerWith = (schemaRegistryClient, isKey, properties) =>
        schemaRegistryClient.flatMap { schemaRegistryClient =>
          F.delay {
            val serializer = new KafkaAvroSerializer(schemaRegistryClient)
            serializer.configure(withDefaults(properties), isKey)
            serializer
          }
        }
    )

  def apply[F[_], A](
    schemaRegistryClientSettings: SchemaRegistryClientSettings[F]
  )(implicit F: Sync[F]): AvroSettings[F, A] =
    create(schemaRegistryClientSettings.createSchemaRegistryClient)

  def apply[F[_], A](
    schemaRegistryClient: SchemaRegistryClient
  )(implicit F: Sync[F]): AvroSettings[F, A] =
    create(F.pure(schemaRegistryClient))

  def apply[F[_], A](implicit settings: AvroSettings[F, A]): AvroSettings[F, A] =
    settings

  implicit def avroSettingsShow[F[_], A]: Show[AvroSettings[F, A]] =
    Show.fromToString
}

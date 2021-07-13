/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import _root_.scalapb.JavaProtoSupport
import com.google.protobuf.Message

import scala.annotation.implicitNotFound

package object scalapb {

  /** Alias for `io.confluent.kafka.schemaregistry.client.SchemaRegistryClient`. */
  type SchemaRegistryClient =
    io.confluent.kafka.schemaregistry.client.SchemaRegistryClient

  /** Alias for `io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient`. */
  type CachedSchemaRegistryClient =
    io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient

  /** Alias for `io.confluent.kafka.serializers.KafkaProtobufDeserializer`. */
  type KafkaProtobufDeserializer[T <: Message] =
    io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer[T]

  /** Alias for `io.confluent.kafka.serializers.KafkaProtobufSerializer`. */
  type KafkaProtobufSerializer[T <: Message] =
    io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer[T]

  def protobufDeserializer[ScalaPB, JavaPB <: Message](
    implicit @implicitNotFound("A ScalaPB message with Java conversions is required")
    javaSupport: JavaProtoSupport[ScalaPB, JavaPB]
  ): ProtobufDeserializer[ScalaPB, JavaPB] =
    new ProtobufDeserializer(javaSupport)

  def protobufSerializer[ScalaPB, JavaPB <: Message](
    implicit @implicitNotFound("A ScalaPB message with Java conversions is required")
    javaSupport: JavaProtoSupport[ScalaPB, JavaPB]
  ): ProtobufSerializer[ScalaPB, JavaPB] =
    new ProtobufSerializer[ScalaPB, JavaPB](javaSupport)
}

/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.scalapb

import _root_.scalapb.JavaProtoSupport
import cats.effect.Sync
import cats.implicits._
import com.google.protobuf.Message
import fs2.kafka.{Deserializer, RecordDeserializer}

import scala.annotation.implicitNotFound

final class ProtobufDeserializer[ScalaPB, JavaPB <: Message] private[scalapb] (
  private val companion: JavaProtoSupport[ScalaPB, JavaPB]
) extends AnyVal {
  def using[F[_]](
    settings: ProtobufSettings[F, JavaPB]
  )(implicit F: Sync[F]): RecordDeserializer[F, ScalaPB] = {
    val createDeserializer: Boolean => F[Deserializer[F, ScalaPB]] =
      settings.createProtobufDeserializer(_).map { deserializer =>
        Deserializer.instance { (topic, _, bytes) =>
          F.defer {
            F.delay(deserializer.deserialize(topic, bytes)).map(companion.fromJavaProto)
          }
        }
      }

    RecordDeserializer.instance(
      forKey = createDeserializer(true),
      forValue = createDeserializer(false)
    )
  }

  override def toString: String =
    "ProtobufDeserializer$" + System.identityHashCode(this)
}

object ProtobufDeserializer {

  def apply[ScalaPB, JavaPB <: Message](
    implicit @implicitNotFound("A ScalaPB message with Java conversions is required")
    javaSupport: JavaProtoSupport[ScalaPB, JavaPB]
  ): ProtobufDeserializer[ScalaPB, JavaPB] =
    new ProtobufDeserializer(javaSupport)
}

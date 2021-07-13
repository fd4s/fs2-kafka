/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.scalapb

import cats.effect.Sync
import cats.implicits._
import com.google.protobuf.Message
import fs2.kafka.{RecordSerializer, Serializer}
import scalapb.JavaProtoSupport

import scala.annotation.implicitNotFound

final class ProtobufSerializer[ScalaPB, JavaPB <: Message] private[scalapb] (
  private val javaSupport: JavaProtoSupport[ScalaPB, JavaPB]
) extends AnyVal {
  def using[F[_]](
    settings: ProtobufSettings[F, JavaPB]
  )(implicit F: Sync[F]): RecordSerializer[F, ScalaPB] = {
    val createSerializer: Boolean => F[Serializer[F, ScalaPB]] =
      settings.createProtobufSerializer(_).map { serializer =>
        Serializer.instance { (topic, _, a) =>
          F.pure(serializer.serialize(topic, javaSupport.toJavaProto(a)))
        }
      }

    RecordSerializer.instance(
      forKey = createSerializer(true),
      forValue = createSerializer(false)
    )
  }

  override def toString: String =
    "ProtobufSerializer$" + System.identityHashCode(this)
}

object ProtobufSerializer {
  def apply[ScalaPB, JavaPB <: Message](
    implicit @implicitNotFound("A ScalaPB message with Java conversions is required")
    javaSupport: JavaProtoSupport[ScalaPB, JavaPB]
  ): ProtobufSerializer[ScalaPB, JavaPB] =
    new ProtobufSerializer(javaSupport)
}

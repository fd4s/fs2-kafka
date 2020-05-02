/*
 * Copyright 2018-2020 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan

import java.nio.ByteBuffer

import _root_.vulcan.Codec
import cats.effect.Sync
import cats.implicits._
import fs2.kafka.{Deserializer, RecordDeserializer}

final class AvroDeserializer[A] private[vulcan] (
  private val codec: Codec[A]
) extends AnyVal {
  def using[F[_]](
    settings: AvroSettings[F]
  )(implicit F: Sync[F]): RecordDeserializer[F, A] =
    codec.schema match {
      case Right(schema) =>
        val createDeserializer: Boolean => F[Deserializer[F, A]] =
          settings.createAvroDeserializer(_).map {
            case (deserializer, schemaRegistryClient) =>
              Deserializer.instance { (topic, _, bytes) =>
                F.suspend {
                  val writerSchemaId = ByteBuffer.wrap(bytes).getInt(1) // skip magic byte
                  val writerSchema = schemaRegistryClient.getById(writerSchemaId)

                  codec.decode(deserializer.deserialize(topic, bytes, schema), writerSchema) match {
                    case Right(a)    => F.pure(a)
                    case Left(error) => F.raiseError(error.throwable)
                  }
                }
              }
          }

        RecordDeserializer.instance(
          forKey = createDeserializer(true),
          forValue = createDeserializer(false)
        )

      case Left(error) =>
        RecordDeserializer.const {
          F.raiseError(error.throwable)
        }
    }

  override def toString: String =
    "AvroDeserializer$" + System.identityHashCode(this)
}

object AvroDeserializer {
  def apply[A](implicit codec: Codec[A]): AvroDeserializer[A] =
    new AvroDeserializer(codec)

  /**
    * Creates a single RecordDeserializer that can handle multiple topics each with its own codec
    *
    * @param settings avro configuration such as for the schema registry
    * @param pair the first topic, codec pair
    * @param pairs subsequent topic, codec pairs
    * @tparam F the type in which effects will be suspended
    * @tparam A the type which this deserializer will produce
    * @return a RecordDeserializer incorporating the supplied codecs
    */
  def topics[F[_]: Sync, A](
    settings: AvroSettings[F]
  )(pair: (String, Codec[_ <: A]), pairs: (String, Codec[_ <: A])*): RecordDeserializer[F, A] = {
    val all = pair :: pairs.toList
    val desers = all.traverse {
      case (topic, codec) =>
        avroDeserializer(codec).using(settings).forValue.map(c => (topic, c.widen[A]))
    }

    val deserializer = desers.map { des =>
      val fn = des.foldLeft(PartialFunction.empty[String, Deserializer[F, A]])((acc, p) => {
        acc.orElse({
          case p._1 => p._2
        })
      })
      Deserializer.topic(fn)
    }

    RecordDeserializer.const(deserializer)
  }
}

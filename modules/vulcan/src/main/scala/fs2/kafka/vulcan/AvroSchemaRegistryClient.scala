package fs2.kafka.vulcan

import cats.effect.Sync
import cats.syntax.all._
import fs2.kafka.{
  Deserializer,
  KeyDeserializer,
  KeySerializer,
  Serializer,
  ValueDeserializer,
  ValueSerializer
}
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import vulcan.Codec

import java.nio.ByteBuffer

sealed abstract class AvroSchemaRegistryClient[F[_]] {
  def keyDeserializer[A](implicit codec: Codec[A]): KeyDeserializer[F, A]
  def valueDeserializer[A](implicit codec: Codec[A]): ValueDeserializer[F, A]

  def keySerializer[A](implicit codec: Codec[A]): KeySerializer[F, A]
  def valueSerializer[A](implicit codec: Codec[A]): ValueSerializer[F, A]
}

object AvroSchemaRegistryClient {
  def apply[F[_]](settings: AvroSettings[F])(implicit F: Sync[F]): F[AvroSchemaRegistryClient[F]] =
    settings.schemaRegistryClient.flatMap { src =>
      val memoizedSettings = settings.withSchemaRegistryClient(F.pure(src))
      (
        memoizedSettings.createAvroSerializer(isKey = true).map(_._1),
        memoizedSettings.createAvroSerializer(isKey = false).map(_._1),
        memoizedSettings.createAvroDeserializer(isKey = true).map(_._1),
        memoizedSettings.createAvroDeserializer(isKey = false).map(_._1)
      ).mapN { (keySerializer, valueSerializer, keyDeserializer, valueDeserializer) =>
        apply(src, keySerializer, valueSerializer, keyDeserializer, valueDeserializer)
      }
    }

  def apply[F[_]: Sync](
    schemaRegistryClient: SchemaRegistryClient,
    javaKeySerializer: KafkaAvroSerializer,
    javaValueSerializer: KafkaAvroSerializer,
    javaKeyDeserializer: KafkaAvroDeserializer,
    javaValueDeserializer: KafkaAvroDeserializer
  ): AvroSchemaRegistryClient[F] = new AvroSchemaRegistryClient[F] {
    def keyDeserializer[A](implicit codec: Codec[A]): KeyDeserializer[F, A] =
      createDeserializer(codec, javaKeyDeserializer, schemaRegistryClient)

    def valueDeserializer[A](implicit codec: Codec[A]): ValueDeserializer[F, A] =
      createDeserializer(codec, javaValueDeserializer, schemaRegistryClient)

    def keySerializer[A](implicit codec: Codec[A]): KeySerializer[F, A] =
      createSerializer(codec, javaKeySerializer)

    def valueSerializer[A](implicit codec: Codec[A]): ValueSerializer[F, A] =
      createSerializer(codec, javaValueSerializer)
  }

  private def createSerializer[F[_], A](codec: Codec[A], javaSerializer: KafkaAvroSerializer)(
    implicit F: Sync[F]
  ): Serializer[F, A] =
    Serializer.instance { (topic, _, a) =>
      codec
        .encode(a)
        .leftMap(_.throwable)
        .liftTo[F]
        .flatMap { jAvro =>
          F.delay(javaSerializer.serialize(topic, jAvro))
        }
    }

  private def createDeserializer[F[_], A](
    codec: Codec[A],
    javaDeserializer: KafkaAvroDeserializer,
    schemaRegistryClient: SchemaRegistryClient
  )(implicit F: Sync[F]): Deserializer[F, A] =
    codec.schema match {
      case Left(err) => Deserializer.fail(err.throwable)
      case Right(schema) =>
        Deserializer.instance { (topic, _, bytes) =>
          F.defer {
            val writerSchemaId =
              ByteBuffer.wrap(bytes).getInt(1) // skip magic byte

            val writerSchema = {
              val schema = schemaRegistryClient.getSchemaById(writerSchemaId)
              if (schema.isInstanceOf[AvroSchema])
                schema.asInstanceOf[AvroSchema].rawSchema()
              else
                null
            }

            codec
              .decode(javaDeserializer.deserialize(topic, bytes, schema), writerSchema)
              .leftMap(_.throwable)
              .liftTo[F]
          }
        }
    }
}

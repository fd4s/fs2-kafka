/*
 * Copyright 2018-2020 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.Apply
import cats.effect._
import cats.effect.syntax.all._
import cats.implicits._
import fs2.kafka.internal._
import fs2.kafka.internal.converters.collection._
import org.apache.kafka.clients.producer.{Callback, RecordMetadata}
import org.apache.kafka.common.{Metric, MetricName}

/**
  * [[KafkaProducer]] represents a producer of Kafka records, with the
  * ability to produce `ProducerRecord`s using [[produce]]. Records are
  * wrapped in [[ProducerRecords]] which allow an arbitrary value, that
  * is a passthrough, to be included in the result. Most often this is
  * used for keeping the [[CommittableOffset]]s, in order to commit
  * offsets, but any value can be used as passthrough value.
  */
abstract class KafkaProducer[F[_], K, V] {

  /**
    * Produces the specified [[ProducerRecords]] in two steps: the
    * first effect puts the records in the buffer of the producer,
    * and the second effect waits for the records to send.
    *
    * It's possible to `flatten` the result from this function to
    * have an effect which both sends the records and waits for
    * them to finish sending.
    *
    * Waiting for individual records to send can substantially
    * limit performance. In some cases, this is necessary, and
    * so we might want to consider the following alternatives.
    *
    * - Wait for the produced records in batches, improving
    *   the rate at which records are produced, but loosing
    *   the guarantee where `produce >> otherAction` means
    *   `otherAction` executes after the record has been
    *   sent.<br>
    * - Run several `produce.flatten >> otherAction` concurrently,
    *   improving the rate at which records are produced, and still
    *   have `otherAction` execute after records have been sent,
    *   but losing the order of produced records.
    */
  def produce[P](
    records: ProducerRecords[K, V, P]
  ): F[F[ProducerResult[K, V, P]]]
}

object KafkaProducer {

  /**
    * [[KafkaProducer.Metrics]] extends [[KafkaProducer]] to provide
    * access to the underlying producer metrics.
    */
  abstract class Metrics[F[_], K, V] extends KafkaProducer[F, K, V] {

    /**
      * Returns producer metrics.
      *
      * @see org.apache.kafka.clients.producer.KafkaProducer#metrics
      */
    def metrics: F[Map[MetricName, Metric]]
  }

  private[kafka] def resource[F[_], K, V](
    settings: ProducerSettings[F, K, V]
  )(
    implicit F: Concurrent[F],
    context: ContextShift[F]
  ): Resource[F, KafkaProducer.Metrics[F, K, V]] =
    Connection.resource(settings).evalMap(_.withSerializersFrom(settings))

  private[kafka] def produceRecord[F[_], K, V](
    keySerializer: Serializer[F, K],
    valueSerializer: Serializer[F, V],
    producer: KafkaByteProducer
  )(
    implicit F: Concurrent[F],
    context: ContextShift[F]
  ): ProducerRecord[K, V] => F[F[(ProducerRecord[K, V], RecordMetadata)]] =
    record =>
      asJavaRecord(keySerializer, valueSerializer, record).flatMap { javaRecord =>
        F.async { (cb: Either[Throwable, (ProducerRecord[K, V], RecordMetadata)] => Unit) =>
            producer.send(
              javaRecord,
              callback { (metadata, exception) =>
                cb {
                  if (exception == null)
                    Right((record, metadata))
                  else Left(exception)
                }
              }
            )
            ()
          }
          .guarantee(context.shift)
          .start
          .map(_.join)
      }

  private[this] def serializeToBytes[F[_], K, V](
    keySerializer: Serializer[F, K],
    valueSerializer: Serializer[F, V],
    record: ProducerRecord[K, V]
  )(implicit F: Apply[F]): F[(Array[Byte], Array[Byte])] = {
    val keyBytes =
      keySerializer.serialize(record.topic, record.headers, record.key)

    val valueBytes =
      valueSerializer.serialize(record.topic, record.headers, record.value)

    keyBytes.product(valueBytes)
  }

  private[this] def asJavaRecord[F[_], K, V](
    keySerializer: Serializer[F, K],
    valueSerializer: Serializer[F, V],
    record: ProducerRecord[K, V]
  )(implicit F: Apply[F]): F[KafkaByteProducerRecord] =
    serializeToBytes(keySerializer, valueSerializer, record).map {
      case (keyBytes, valueBytes) =>
        new KafkaByteProducerRecord(
          record.topic,
          record.partition.fold[java.lang.Integer](null)(identity),
          record.timestamp.fold[java.lang.Long](null)(identity),
          keyBytes,
          valueBytes,
          record.headers.asJava
        )
    }

  private[this] def callback(f: (RecordMetadata, Exception) => Unit): Callback =
    new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit =
        f(metadata, exception)
    }

  sealed abstract class Connection[F[_]] {
    def withSerializers[K, V](
      keySerializer: Serializer[F, K],
      valueSerializer: Serializer[F, V]
    ): KafkaProducer.Metrics[F, K, V]

    def withSerializersFrom[K, V](
      settings: ProducerSettings[F, K, V]
    ): F[KafkaProducer.Metrics[F, K, V]]
  }

  object Connection {
    def resource[F[_]](
      settings: ProducerSettings[F, _, _]
    )(
      implicit F: Concurrent[F],
      context: ContextShift[F]
    ): Resource[F, Connection[F]] =
      WithProducer(settings).map { withProducer =>
        new Connection[F] {
          override def withSerializers[K, V](
            keySerializer: Serializer[F, K],
            valueSerializer: Serializer[F, V]
          ): KafkaProducer.Metrics[F, K, V] =
            new KafkaProducer.Metrics[F, K, V] {
              override def produce[P](
                records: ProducerRecords[K, V, P]
              ): F[F[ProducerResult[K, V, P]]] = {
                withProducer { (producer, _) =>
                  records.records
                    .traverse(produceRecord(keySerializer, valueSerializer, producer))
                    .map(_.sequence.map(ProducerResult(_, records.passthrough)))
                }
              }

              override def metrics: F[Map[MetricName, Metric]] =
                withProducer.blocking { _.metrics().asScala.toMap }

              override def toString: String =
                "KafkaProducer$" + System.identityHashCode(this)
            }

          override def withSerializersFrom[K, V](
            settings: ProducerSettings[F, K, V]
          ): F[KafkaProducer.Metrics[F, K, V]] =
            (settings.keySerializer, settings.valueSerializer).mapN(withSerializers)
        }
      }
  }
}

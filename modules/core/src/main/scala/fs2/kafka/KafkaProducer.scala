/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.Apply
import cats.effect._
import cats.implicits._
import fs2._
import fs2.kafka.internal._
import fs2.kafka.internal.converters.collection._
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.{Metric, MetricName}
import cats.effect.std.Dispatcher
import fs2.kafka.producer.MkProducer

import scala.annotation.nowarn

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
    records: ProducerRecords[P, K, V]
  ): F[F[ProducerResult[P, K, V]]]
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

  /**
    * Creates a new [[KafkaProducer]] in the `Resource` context,
    * using the specified [[ProducerSettings]]. Note that there
    * is another version where `F[_]` is specified explicitly and
    * the key and value type can be inferred, which allows you
    * to use the following syntax.
    *
    * {{{
    * KafkaProducer.resource[F].using(settings)
    * }}}
    */
  def resource[F[_], K, V](
    settings: ProducerSettings[F, K, V]
  )(implicit F: Async[F], mk: MkProducer[F]): Resource[F, KafkaProducer.Metrics[F, K, V]] =
    KafkaProducerConnection.resource(settings)(F, mk).evalMap(_.withSerializersFrom(settings))

  private[kafka] def from[F[_]: Async, K, V](
    withProducer: WithProducer[F],
    keySerializer: Serializer[F, K],
    valueSerializer: Serializer[F, V]
  )(implicit dispatcher: Dispatcher[F]): KafkaProducer.Metrics[F, K, V] =
    new KafkaProducer.Metrics[F, K, V] {
      override def produce[P](
        records: ProducerRecords[P, K, V]
      ): F[F[ProducerResult[P, K, V]]] =
        withProducer { (producer, _) =>
          records.records
            .traverse(produceRecord(keySerializer, valueSerializer, producer))
            .map(_.sequence.map(ProducerResult(_, records.passthrough)))
        }

      override def metrics: F[Map[MetricName, Metric]] =
        withProducer.blocking { _.metrics().asScala.toMap }

      override def toString: String =
        "KafkaProducer$" + System.identityHashCode(this)
    }

  /**
    * Creates a new [[KafkaProducer]] in the `Stream` context,
    * using the specified [[ProducerSettings]]. Note that there
    * is another version where `F[_]` is specified explicitly and
    * the key and value type can be inferred, which allows you
    * to use the following syntax.
    *
    * {{{
    * KafkaProducer.stream[F].using(settings)
    * }}}
    */
  def stream[F[_], K, V](
    settings: ProducerSettings[F, K, V]
  )(implicit F: Async[F], mk: MkProducer[F]): Stream[F, KafkaProducer.Metrics[F, K, V]] =
    Stream.resource(KafkaProducer.resource(settings)(F, mk))

  private[kafka] def produceRecord[F[_], K, V](
    keySerializer: Serializer[F, K],
    valueSerializer: Serializer[F, V],
    producer: KafkaByteProducer
  )(
    implicit F: Async[F],
    dispatcher: Dispatcher[F]
  ): ProducerRecord[K, V] => F[F[(ProducerRecord[K, V], RecordMetadata)]] =
    record =>
      asJavaRecord(keySerializer, valueSerializer, record).flatMap { javaRecord =>
        Deferred[F, Either[Throwable, (ProducerRecord[K, V], RecordMetadata)]].flatMap { deferred =>
          F.blocking {
              producer.send(
                javaRecord, { (metadata, exception) =>
                  val complete =
                    deferred.complete {
                      if (exception == null)
                        Right((record, metadata))
                      else Left(exception)
                    }.void
                  dispatcher.unsafeRunSync(complete)
                }
              )
            }
            .as(deferred.get.rethrow)
        }
      }

  /**
    * Creates a [[KafkaProducer]] using the provided settings and
    * produces record in batches, limiting the number of records
    * in the same batch using [[ProducerSettings#parallelism]].
    */
  def pipe[F[_], K, V, P](
    settings: ProducerSettings[F, K, V]
  )(
    implicit F: Async[F],
    mk: MkProducer[F]
  ): Pipe[F, ProducerRecords[P, K, V], ProducerResult[P, K, V]] =
    records => stream(settings)(F, mk).flatMap(pipe(settings, _).apply(records))

  /**
    * Produces records in batches using the provided [[KafkaProducer]].
    * The number of records in the same batch is limited using the
    * [[ProducerSettings#parallelism]] setting.
    */
  def pipe[F[_]: Concurrent, K, V, P](
    settings: ProducerSettings[F, K, V],
    producer: KafkaProducer[F, K, V]
  ): Pipe[F, ProducerRecords[P, K, V], ProducerResult[P, K, V]] =
    _.evalMap(producer.produce).mapAsync(settings.parallelism)(identity)

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

  def apply[F[_]]: ProducerPartiallyApplied[F] =
    new ProducerPartiallyApplied

  private[kafka] final class ProducerPartiallyApplied[F[_]](val dummy: Boolean = true)
      extends AnyVal {

    /**
      * Alternative version of `resource` where the `F[_]` is
      * specified explicitly, and where the key and value type can
      * be inferred from the [[ProducerSettings]]. This allows you
      * to use the following syntax.
      *
      * {{{
      * KafkaProducer[F].resource(settings)
      * }}}
      */
    def resource[K, V](settings: ProducerSettings[F, K, V])(
      implicit F: Async[F],
      mk: MkProducer[F]
    ): Resource[F, KafkaProducer[F, K, V]] =
      KafkaProducer.resource(settings)(F, mk)

    /**
      * Alternative version of `stream` where the `F[_]` is
      * specified explicitly, and where the key and value type can
      * be inferred from the [[ProducerSettings]]. This allows you
      * to use the following syntax.
      *
      * {{{
      * KafkaProducer[F].stream(settings)
      * }}}
      */
    def stream[K, V](settings: ProducerSettings[F, K, V])(
      implicit F: Async[F],
      mk: MkProducer[F]
    ): Stream[F, KafkaProducer[F, K, V]] =
      KafkaProducer.stream(settings)(F, mk)

    override def toString: String =
      "ProducerPartiallyApplied$" + System.identityHashCode(this)
  }

  /*
   * Prevents the default `MkProducer` instance from being implicitly available
   * to code defined in this object, ensuring factory methods require an instance
   * to be provided at the call site.
   */
  @nowarn("cat=unused")
  implicit private def mkAmbig1[F[_]]: MkProducer[F] =
    throw new AssertionError("should not be used")
  @nowarn("cat=unused")
  implicit private def mkAmbig2[F[_]]: MkProducer[F] =
    throw new AssertionError("should not be used")
}

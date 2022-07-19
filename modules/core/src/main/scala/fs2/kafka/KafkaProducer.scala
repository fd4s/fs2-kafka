/*
 * Copyright 2018-2022 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect._
import cats.syntax.all._
import cats.{Apply, Functor, Traverse}
import fs2._
import fs2.kafka.internal._
import fs2.kafka.producer.MkProducer
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.{Metric, MetricName}

import scala.annotation.nowarn
import scala.concurrent.Promise

/**
  * [[KafkaProducer]] represents a producer of Kafka records, with the
  * ability to produce `ProducerRecord`s using [[produce]].
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
  def produce[Coll[_]: Traverse](
    records: ProducerRecords[Coll, K, V]
  ): F[F[ProducerResult[Coll, K, V]]]
}

object KafkaProducer {

  implicit class ProducerOps[F[_], K, V](private val producer: KafkaProducer[F, K, V])
      extends AnyVal {

    /**
      * Produce a single [[ProducerRecord]], see [[KafkaProducer.produce]] for general semantics.
      */
    def produceOne_(record: ProducerRecord[K, V])(implicit F: Functor[F]): F[F[RecordMetadata]] =
      produceOne(record).map(_.map { res =>
        res._2
      })

    /**
      * Produce a single record to the specified topic using the provided key and value,
      * see [[KafkaProducer.produce]] for general semantics.
      */
    def produceOne_(topic: String, key: K, value: V)(implicit F: Functor[F]): F[F[RecordMetadata]] =
      produceOne_(ProducerRecord(topic, key, value))

    /**
      * Produce a single record to the specified topic using the provided key and value,
      * see [[KafkaProducer.produce]] for general semantics.
      */
    def produceOne(
      topic: String,
      key: K,
      value: V
    ): F[F[ProducerResult[cats.Id, K, V]]] =
      produceOne(ProducerRecord(topic, key, value))

    /**
      * Produce a single [[ProducerRecord]], see [[KafkaProducer.produce]] for general semantics.
      */
    def produceOne(record: ProducerRecord[K, V]): F[F[ProducerResult[cats.Id, K, V]]] =
      producer.produce(ProducerRecords.one(record))

  }

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
    KafkaProducerConnection.resource(settings).evalMap(_.withSerializersFrom(settings))

  private[kafka] def from[F[_], K, V](
    connection: KafkaProducerConnection[F],
    keySerializer: KeySerializer[F, K],
    valueSerializer: ValueSerializer[F, V]
  ): KafkaProducer.Metrics[F, K, V] =
    new KafkaProducer.Metrics[F, K, V] {
      def produce[Coll[_]: Traverse](
        records: ProducerRecords[Coll, K, V]
      ): F[F[ProducerResult[Coll, K, V]]] =
        connection.produce(records)(Traverse[Coll], keySerializer, valueSerializer)

      override def metrics: F[Map[MetricName, Metric]] =
        connection.metrics

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
    Stream.resource(KafkaProducer.resource(settings))

  private[kafka] def produce[F[_]: Async, G[_]: Traverse, K, V](
    withProducer: WithProducer[F],
    keySerializer: KeySerializer[F, K],
    valueSerializer: ValueSerializer[F, V],
    records: G[ProducerRecord[K, V]]
  ): F[F[G[(ProducerRecord[K, V], RecordMetadata)]]] =
    withProducer { (producer, blocking) =>
      records
        .traverse(produceRecord(keySerializer, valueSerializer, producer, blocking))
        .map(_.sequence)
    }

  private[kafka] def produceRecord[F[_], K, V](
    keySerializer: KeySerializer[F, K],
    valueSerializer: ValueSerializer[F, V],
    producer: KafkaByteProducer,
    blocking: Blocking[F]
  )(
    implicit F: Async[F]
  ): ProducerRecord[K, V] => F[F[(ProducerRecord[K, V], RecordMetadata)]] =
    record =>
      asJavaRecord(keySerializer, valueSerializer, record).flatMap { javaRecord =>
        F.delay(Promise[(ProducerRecord[K, V], RecordMetadata)]()).flatMap { promise =>
          blocking {
            producer.send(
              javaRecord, { (metadata, exception) =>
                if (exception == null)
                  promise.success((record, metadata))
                else promise.failure(exception)
              }
            )
          }.as(F.fromFuture(F.delay(promise.future)))
        }
      }

  /**
    * Creates a [[KafkaProducer]] using the provided settings and
    * produces record in batches.
    */
  def pipe[F[_], Coll[_]: Traverse, K, V](
    settings: ProducerSettings[F, K, V]
  )(
    implicit F: Async[F],
    mk: MkProducer[F]
  ): Pipe[F, ProducerRecords[Coll, K, V], ProducerResult[Coll, K, V]] =
    records => stream(settings).flatMap(pipe(_).apply(records))

  /**
    * Produces records in batches using the provided [[KafkaProducer]].
    */
  def pipe[F[_]: Concurrent, Coll[_]: Traverse, K, V](
    producer: KafkaProducer[F, K, V]
  ): Pipe[F, ProducerRecords[Coll, K, V], ProducerResult[Coll, K, V]] =
    _.evalMap(producer.produce[Coll](_)).parEvalMap(Int.MaxValue)(identity)

  private[this] def serializeToBytes[F[_], K, V](
    keySerializer: KeySerializer[F, K],
    valueSerializer: ValueSerializer[F, V],
    record: ProducerRecord[K, V]
  )(implicit F: Apply[F]): F[(Array[Byte], Array[Byte])] = {
    val keyBytes =
      keySerializer.serialize(record.topic, record.headers, record.key)

    val valueBytes =
      valueSerializer.serialize(record.topic, record.headers, record.value)

    keyBytes.product(valueBytes)
  }

  private[this] def asJavaRecord[F[_], K, V](
    keySerializer: KeySerializer[F, K],
    valueSerializer: ValueSerializer[F, V],
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
      KafkaProducer.resource(settings)

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
      KafkaProducer.stream(settings)

    override def toString: String =
      "ProducerPartiallyApplied$" + System.identityHashCode(this)
  }

  /*
   * Prevents the default `MkProducer` instance from being implicitly available
   * to code defined in this object, ensuring factory methods require an instance
   * to be provided at the call site.
   */
  @nowarn("msg=never used")
  implicit private def mkAmbig1[F[_]]: MkProducer[F] =
    throw new AssertionError("should not be used")
  @nowarn("msg=never used")
  implicit private def mkAmbig2[F[_]]: MkProducer[F] =
    throw new AssertionError("should not be used")
}

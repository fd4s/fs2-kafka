/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.syntax.all._
import cats.effect.{Async, Resource, Outcome}
import cats.syntax.all._
import fs2.kafka.internal._
import scala.jdk.CollectionConverters._
import fs2.kafka.producer.MkProducer
import fs2.{Chunk, Stream}
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.{Metric, MetricName}

import scala.annotation.nowarn

/**
  * Represents a producer of Kafka records specialized for 'read-process-write'
  * streams, with the ability to atomically produce `ProducerRecord`s and commit
  * corresponding [[CommittableOffset]]s using [[produce]].<br>
  * <br>
  * Records are wrapped in [[TransactionalProducerRecords]] which allow an
  * arbitrary passthrough value to be included in the result.
  */
abstract class TransactionalKafkaProducer[F[_], K, V] {

  /**
    * Produces the `ProducerRecord`s in the specified [[TransactionalProducerRecords]]
    * in four steps: first a transaction is initialized, then the records are placed
    * in the buffer of the producer, then the offsets of the records are sent to the
    * transaction, and lastly the transaction is committed. If errors or cancellation
    * occurs, the transaction is aborted. The returned effect succeeds if the whole
    * transaction completes successfully.
    */
  def produce[P](
    records: TransactionalProducerRecords[F, P, K, V]
  ): F[ProducerResult[P, K, V]]
}

object TransactionalKafkaProducer {

  /**
    * [[TransactionalKafkaProducer.Metrics]] extends [[TransactionalKafkaProducer]] to provide
    * access to the underlying producer metrics.
    */
  abstract class Metrics[F[_], K, V] extends TransactionalKafkaProducer[F, K, V] {

    /**
      * Returns producer metrics.
      *
      * @see org.apache.kafka.clients.producer.KafkaProducer#metrics
      */
    def metrics: F[Map[MetricName, Metric]]
  }

  /**
    * Creates a new [[TransactionalKafkaProducer]] in the `Resource` context,
    * using the specified [[TransactionalProducerSettings]]. Note that there
    * is another version where `F[_]` is specified explicitly and the key and
    * value type can be inferred, which allows you to use the following syntax.
    *
    * {{{
    * TransactionalKafkaProducer.resource[F].using(settings)
    * }}}
    */
  def resource[F[_], K, V](
    settings: TransactionalProducerSettings[F, K, V]
  )(
    implicit F: Async[F],
    mk: MkProducer[F]
  ): Resource[F, TransactionalKafkaProducer.Metrics[F, K, V]] =
    (
      Resource.eval(settings.producerSettings.keySerializer),
      Resource.eval(settings.producerSettings.valueSerializer),
      WithProducer(mk, settings)
    ).mapN { (keySerializer, valueSerializer, withProducer) =>
      new TransactionalKafkaProducer.Metrics[F, K, V] {
        override def produce[P](
          records: TransactionalProducerRecords[F, P, K, V]
        ): F[ProducerResult[P, K, V]] =
          produceTransaction(records)
            .map(ProducerResult(_, records.passthrough))

        private[this] def produceTransaction[P](
          records: TransactionalProducerRecords[F, P, K, V]
        ): F[Chunk[(ProducerRecord[K, V], RecordMetadata)]] =
          if (records.records.isEmpty) F.pure(Chunk.empty)
          else {
            val batch =
              CommittableOffsetBatch.fromFoldableMap(records.records)(_.offset)

            val consumerGroupId =
              if (batch.consumerGroupIdsMissing || batch.consumerGroupIds.size != 1)
                F.raiseError(ConsumerGroupException(batch.consumerGroupIds))
              else F.pure(batch.consumerGroupIds.head)

            consumerGroupId.flatMap { groupId =>
              withProducer { (producer, blocking) =>
                blocking(producer.beginTransaction())
                  .bracketCase { _ =>
                    records.records
                      .flatMap(_.records)
                      .traverse(
                        KafkaProducer
                          .produceRecord(keySerializer, valueSerializer, producer, blocking)
                      )
                      .map(_.sequence)
                      .flatTap { _ =>
                        blocking {
                          producer.sendOffsetsToTransaction(
                            batch.offsets.asJava,
                            groupId
                          )
                        }
                      }
                  } {
                    case (_, Outcome.Succeeded(_)) =>
                      blocking(producer.commitTransaction())
                    case (_, Outcome.Canceled() | Outcome.Errored(_)) =>
                      blocking(producer.abortTransaction())
                  }
              }.flatten
            }
          }

        override def metrics: F[Map[MetricName, Metric]] =
          withProducer.blocking { _.metrics().asScala.toMap }

        override def toString: String =
          "TransactionalKafkaProducer$" + System.identityHashCode(this)
      }
    }

  /**
    * Creates a new [[TransactionalKafkaProducer]] in the `Stream` context,
    * using the specified [[TransactionalProducerSettings]]. Note that there
    * is another version where `F[_]` is specified explicitly and the key and
    * value type can be inferred, which allows you to use the following syntax.
    *
    * {{{
    * TransactionalKafkaProducer.stream[F].using(settings)
    * }}}
    */
  def stream[F[_], K, V](
    settings: TransactionalProducerSettings[F, K, V]
  )(
    implicit F: Async[F],
    mk: MkProducer[F]
  ): Stream[F, TransactionalKafkaProducer.Metrics[F, K, V]] =
    Stream.resource(resource(settings))

  def apply[F[_]]: TransactionalProducerPartiallyApplied[F] =
    new TransactionalProducerPartiallyApplied

  private[kafka] final class TransactionalProducerPartiallyApplied[F[_]](val dummy: Boolean = true)
      extends AnyVal {

    /**
      * Alternative version of `resource` where the `F[_]` is
      * specified explicitly, and where the key and value type can
      * be inferred from the [[TransactionalProducerSettings]]. This allows you
      * to use the following syntax.
      *
      * {{{
      * KafkaProducer[F].resource(settings)
      * }}}
      */
    def resource[K, V](settings: TransactionalProducerSettings[F, K, V])(
      implicit F: Async[F],
      mk: MkProducer[F]
    ): Resource[F, TransactionalKafkaProducer.Metrics[F, K, V]] =
      TransactionalKafkaProducer.resource(settings)

    /**
      * Alternative version of `stream` where the `F[_]` is
      * specified explicitly, and where the key and value type can
      * be inferred from the [[TransactionalProducerSettings]]. This allows you
      * to use the following syntax.
      *
      * {{{
      * KafkaProducer[F].stream(settings)
      * }}}
      */
    def stream[K, V](settings: TransactionalProducerSettings[F, K, V])(
      implicit F: Async[F],
      mk: MkProducer[F]
    ): Stream[F, TransactionalKafkaProducer.Metrics[F, K, V]] =
      TransactionalKafkaProducer.stream(settings)

    override def toString: String =
      "TransactionalProducerPartiallyApplied$" + System.identityHashCode(this)
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

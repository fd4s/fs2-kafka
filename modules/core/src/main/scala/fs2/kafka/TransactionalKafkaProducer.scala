/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.{Concurrent, ContextShift, ExitCase, Resource}
import cats.effect.syntax.all._
import cats.implicits._
import fs2.Chunk
import fs2.kafka.internal._
import fs2.kafka.internal.converters.collection._
import org.apache.kafka.clients.producer.RecordMetadata

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
    records: TransactionalProducerRecords[F, K, V, P]
  ): F[ProducerResult[K, V, P]]
}

private[kafka] object TransactionalKafkaProducer {
  def resource[F[_], K, V](
    settings: TransactionalProducerSettings[F, K, V]
  )(
    implicit F: Concurrent[F],
    context: ContextShift[F]
  ): Resource[F, TransactionalKafkaProducer[F, K, V]] =
    Resource.liftF(settings.producerSettings.keySerializer).flatMap { keySerializer =>
      Resource.liftF(settings.producerSettings.valueSerializer).flatMap { valueSerializer =>
        WithProducer(settings).map { withProducer =>
          new TransactionalKafkaProducer[F, K, V] {
            override def produce[P](
              records: TransactionalProducerRecords[F, K, V, P]
            ): F[ProducerResult[K, V, P]] =
              produceTransaction(records)
                .map(ProducerResult(_, records.passthrough))

            private[this] def produceTransaction[P](
              records: TransactionalProducerRecords[F, K, V, P]
            ): F[Chunk[(ProducerRecord[K, V], RecordMetadata)]] = {
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
                            KafkaProducer.produceRecord(keySerializer, valueSerializer, producer)
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
                        case (_, ExitCase.Completed) =>
                          blocking(producer.commitTransaction())
                        case (_, ExitCase.Canceled | ExitCase.Error(_)) =>
                          blocking(producer.abortTransaction())
                      }
                  }.flatten
                }
              }
            }

            override def toString: String =
              "TransactionalKafkaProducer$" + System.identityHashCode(this)
          }
        }
      }
    }
}

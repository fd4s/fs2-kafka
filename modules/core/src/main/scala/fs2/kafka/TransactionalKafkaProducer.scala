/*
 * Copyright 2018-2019 OVO Energy Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fs2.kafka

import cats.Parallel
import cats.effect.{Blocker, ConcurrentEffect, ContextShift, ExitCase, Resource}
import cats.implicits._
import fs2.Chunk
import fs2.kafka.internal._
import fs2.kafka.internal.syntax._
import fs2.kafka.internal.converters.collection._
import org.apache.kafka.clients.producer.{ProducerConfig, RecordMetadata}

import scala.concurrent.duration.FiniteDuration

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
  ): F[ProducerResult[Chunk, K, V, P]]
}

private[kafka] object TransactionalKafkaProducer {

  /**
    * Support class capturing core functionality required to implement
    * a [[TransactionalKafkaProducer]], allowing users to decide how transactional
    * IDs are computed as a follow-up step to registering producer settings.
    */
  class Builder[F[_], G[_], K, V] private[kafka] (
    settings: ProducerSettings[F, K, V],
    transactionTimeout: Option[FiniteDuration]
  )(
    implicit F: ConcurrentEffect[F],
    P: Parallel[F, G],
    context: ContextShift[F]
  ) {

    type Records = CommittableProducerRecords[F, K, V]
    type RecordsChunk = Chunk[Records]
    type RecordGrouper = RecordsChunk => F[Chunk[(KafkaByteProducer, RecordsChunk)]]

    private[this] def resource(
      buildGrouper: Blocker => Resource[F, RecordGrouper]
    ): Resource[F, TransactionalKafkaProducer[F, K, V]] =
      Resource.liftF(settings.keySerializer).flatMap { keySerializer =>
        Resource.liftF(settings.valueSerializer).flatMap { valueSerializer =>
          settings.blocker.fold(Blockers.producer)(Resource.pure[F, Blocker]).flatMap { blocker =>
            buildGrouper(blocker).map { grouper =>
              new TransactionalKafkaProducer[F, K, V] {
                override def produce[P](
                  records: TransactionalProducerRecords[F, K, V, P]
                ): F[ProducerResult[Chunk, K, V, P]] =
                  grouper(records.records)
                    .flatMap { recordGroups =>
                      recordGroups.parFlatTraverse {
                        case (producer, recordGroup) =>
                          produceTransaction(producer, recordGroup)
                      }
                    }
                    .map(ProducerResult(_, records.passthrough))

                private[this] def produceTransaction(
                  producer: KafkaByteProducer,
                  records: RecordsChunk
                ): F[Chunk[(ProducerRecord[K, V], RecordMetadata)]] =
                  if (records.isEmpty) {
                    F.pure(Chunk.empty)
                  } else {
                    val batch = CommittableOffsetBatch.fromFoldableMap(records)(_.offset)

                    val consumerGroupId =
                      if (batch.consumerGroupIdsMissing || batch.consumerGroupIds.size != 1) {
                        F.raiseError[String](ConsumerGroupException(batch.consumerGroupIds))
                      } else {
                        F.pure(batch.consumerGroupIds.head)
                      }

                    consumerGroupId.flatMap { groupId =>
                      records.flatTraverse { committable =>
                        context
                          .blockOn(blocker) {
                            F.bracketCase(F.delay(producer.beginTransaction())) { _ =>
                              committable.records
                                .traverse(
                                  KafkaProducer
                                    .produceRecord(keySerializer, valueSerializer, producer)
                                )
                                .flatMap(_.sequence)
                                .flatTap { _ =>
                                  F.delay {
                                    producer.sendOffsetsToTransaction(
                                      batch.offsets.asJava,
                                      groupId
                                    )
                                  }
                                }
                            } {
                              case (_, ExitCase.Completed) =>
                                F.delay(producer.commitTransaction())
                              case (_, ExitCase.Canceled | ExitCase.Error(_)) =>
                                F.delay(producer.abortTransaction())
                            }
                          }
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

    private def create(id: String): F[KafkaByteProducer] =
      transactionTimeout
        .fold(settings) { timeout =>
          settings.withProperty(
            ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
            timeout.toMillis.toString
          )
        }
        .withProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, id)
        .createProducer

    private def init(blocker: Blocker)(producer: KafkaByteProducer): F[Unit] =
      context.blockOn(blocker)(F.delay(producer.initTransactions()))

    private def close(blocker: Blocker)(producer: KafkaByteProducer): F[Unit] =
      context.blockOn(blocker)(F.delay(producer.close(settings.closeTimeout.asJava)))

    /**
      * Construct a [[TransactionalKafkaProducer]] which uses a constant ID
      * for all transactions.
      *
      * NOTE: This mode only guarantees exactly-once processing if you have a
      * single instance in the consumer group. True exactly-once processing is
      * only guaranteed by running separate producers with unique transactional
      * IDs per topic/partition processed by the group.
      *
      * @param transactionalId the constant ID to use in all transactions initialized
      *                        by the constructed producer
      */
    def withConstantId(transactionalId: String): Resource[F, TransactionalKafkaProducer[F, K, V]] =
      resource { blocker =>
        Resource
          .make(create(transactionalId))(close(blocker))
          .evalTap(init(blocker))
          .map { producer => records =>
            F.pure(Chunk.singleton(producer -> records))
          }
      }
  }
}

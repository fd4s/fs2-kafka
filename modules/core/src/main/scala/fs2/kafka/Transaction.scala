/*
 * Copyright 2018-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.{Async, Ref, Resource}
import cats.effect.kernel.Resource.ExitCase
import fs2.kafka.internal.{Blocking, WithTransactionalProducer}
import cats.implicits._
import fs2.Chunk
import org.apache.kafka.clients.producer.RecordMetadata
import fs2.kafka.internal.converters.collection._

sealed abstract class Transaction[F[_], K, V] {
  def produce[P](records: TransactionalProducerRecords[F, P, K, V]): F[ProducerResult[P, K, V]]
}

object Transaction {
  abstract class WithoutOffsets[F[_], K, V] extends Transaction[F, K, V] {
    def produceWithoutOffsets[P](records: ProducerRecords[P, K, V]): F[ProducerResult[P, K, V]]
  }

  private[this] final class TransactionImpl[F[_], K, V](
    producer: KafkaByteProducer,
    blocking: Blocking[F],
    offsetBatchRef: Ref[F, CommittableOffsetBatch[F]],
    isClosedRef: Ref[F, Boolean],
    keySerializer: Serializer[F, K],
    valueSerializer: Serializer[F, V]
  )(implicit F: Async[F])
      extends Transaction.WithoutOffsets[F, K, V] {
    private[Transaction] def commitTransactionWithOffsets(
      batch: CommittableOffsetBatch[F]
    ): F[Unit] = {
      val batchIsEmpty = batch.offsets.isEmpty

      val sendOffsetBatch = if (batchIsEmpty) {
        F.unit
      } else {
        val consumerGroupId = {
          if (batch.consumerGroupIdsMissing || batch.consumerGroupIds.size != 1)
            F.raiseError(ConsumerGroupException(batch.consumerGroupIds))
          else F.pure(batch.consumerGroupIds.head)
        }

        consumerGroupId.flatMap { groupId =>
          blocking {
            producer.sendOffsetsToTransaction(
              batch.offsets.asJava,
              groupId
            )
          }
        }

      }

      sendOffsetBatch *> blocking(producer.commitTransaction())
    }

    private[Transaction] def close: F[Unit] = isClosedRef.set(true)

    private[this] def stopIfClosed: F[Unit] =
      isClosedRef.get.flatMap { isClosed =>
        if (isClosed) {
          F.raiseError(TransactionLeakedException())
        } else {
          F.unit
        }
      }

    private[this] def produce_(
      records: Chunk[ProducerRecord[K, V]]
    ): F[Chunk[(ProducerRecord[K, V], RecordMetadata)]] =
      records
        .traverse(KafkaProducer.produceRecord(keySerializer, valueSerializer, producer, blocking))
        .map(_.sequence)
        .flatten

    override def produce[P](
      records: TransactionalProducerRecords[F, P, K, V]
    ): F[ProducerResult[P, K, V]] = {
      val incomingOffsetBatch = CommittableOffsetBatch.fromFoldableMap(records.records)(_.offset)

      stopIfClosed *>
        offsetBatchRef.update(currentOffsetBatch => currentOffsetBatch.updated(incomingOffsetBatch)) *>
        produce_(records.records.flatMap(_.records))
          .map(ProducerResult(_, records.passthrough))
    }

    override def produceWithoutOffsets[P](
      records: ProducerRecords[P, K, V]
    ): F[ProducerResult[P, K, V]] =
      stopIfClosed *>
        produce_(records.records).map(ProducerResult(_, records.passthrough))
  }

  private[kafka] def resource[F[_], K, V](
    withProducer: WithTransactionalProducer[F],
    keySerializer: Serializer[F, K],
    valueSerializer: Serializer[F, V]
  )(implicit F: Async[F]): Resource[F, Transaction.WithoutOffsets[F, K, V]] =
    withProducer.semaphore.permit
      .flatMap { _ =>
        Resource
          .makeCase {
            withProducer
              .apply[(Ref[F, CommittableOffsetBatch[F]], TransactionImpl[F, K, V])] {
                case (producer, blocking, _) =>
                  for {
                    _ <- blocking(producer.beginTransaction())
                    offsetBatchRef <- F.ref(CommittableOffsetBatch.empty[F])
                    isClosedRef <- F.ref(false)
                  } yield {
                    val transaction = new TransactionImpl[F, K, V](
                      producer,
                      blocking,
                      offsetBatchRef,
                      isClosedRef,
                      keySerializer,
                      valueSerializer
                    )

                    offsetBatchRef -> transaction
                  }
              }
          } {
            case ((ref, transaction), ExitCase.Succeeded) =>
              transaction.close *> ref.get.flatMap { offsetBatch =>
                transaction
                  .commitTransactionWithOffsets(offsetBatch)
                  .handleErrorWith(_ => withProducer.blocking(_.abortTransaction))
              }
            case ((_, transaction), ExitCase.Errored(_) | ExitCase.Canceled) =>
              transaction.close *> withProducer.blocking(_.abortTransaction)
          }
      }
      .map { case (_, transaction) => transaction }
}

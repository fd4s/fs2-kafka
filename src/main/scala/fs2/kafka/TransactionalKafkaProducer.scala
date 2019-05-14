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

import cats.effect.{ConcurrentEffect, ContextShift, ExitCase, Resource}
import cats.implicits._
import fs2.Chunk
import fs2.kafka.internal._
import org.apache.kafka.clients.producer.RecordMetadata
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
  * Represents a producer of Kafka messages specialized for 'read-process-write'
  * streams, with the ability to atomically produce `ProducerRecord`s and commit
  * corresponding [[CommittableOffset]]s using [[produce]].<br>
  * <br>
  * Records are wrapped in [[TransactionalProducerMessage]] which allow an
  * arbitrary passthrough value to be included in the result.
  */
sealed abstract class TransactionalKafkaProducer[F[_], K, V] {

  /**
    * Produces the `ProducerRecord`s in the specified [[TransactionalProducerMessage]]
    * in four steps: first a transaction is initialized, then the records are placed
    * in the buffer of the producer, then the offsets of the records are sent to the
    * transaction, and lastly the transaction is committed. If errors or cancellation
    * occurs, the transaction is aborted. The returned effect succeeds if the whole
    * transaction completes successfully.<br>
    * <br>
    * If you're only interested in the passthrough value, and not the whole
    * [[ProducerResult]], you can instead use [[producePassthrough]] which
    * only keeps the passthrough value in the output.
    */
  def produce[G[+ _], P](
    message: TransactionalProducerMessage[F, G, K, V, P]
  ): F[ProducerResult[Chunk, K, V, P]]

  /**
    * Like [[produce]] but only keeps the passthrough value of the
    * [[ProducerResult]] rather than the whole [[ProducerResult]].
    */
  def producePassthrough[G[+ _], P](
    message: TransactionalProducerMessage[F, G, K, V, P]
  ): F[P]
}

private[kafka] object TransactionalKafkaProducer {
  def resource[F[_], K, V](
    settings: ProducerSettings[F, K, V]
  )(
    implicit F: ConcurrentEffect[F],
    context: ContextShift[F]
  ): Resource[F, TransactionalKafkaProducer[F, K, V]] =
    WithProducer(settings)
      .evalTap { withProducer =>
        withProducer { producer =>
          F.delay(producer.initTransactions())
        }
      }
      .map { withProducer =>
        new TransactionalKafkaProducer[F, K, V] {
          override def produce[G[+ _], P](
            message: TransactionalProducerMessage[F, G, K, V, P]
          ): F[ProducerResult[Chunk, K, V, P]] =
            produceTransaction(message)
              .map(ProducerResult(_, message.passthrough))

          override def producePassthrough[G[+ _], P](
            message: TransactionalProducerMessage[F, G, K, V, P]
          ): F[P] =
            produceTransaction(message)
              .as(message.passthrough)

          private[this] def produceTransaction[G[+ _], P](
            message: TransactionalProducerMessage[F, G, K, V, P]
          ): F[Chunk[(ProducerRecord[K, V], RecordMetadata)]] = {
            if (message.records.isEmpty) F.pure(Chunk.empty)
            else {
              var consumerGroupIds = Set.empty[String]
              var existsOffsetWithoutGroupId = false
              val batch = CommittableOffsetBatch.fromFoldableMap(message.records) { record =>
                record.committableOffset.consumerGroupId match {
                  case Some(groupId) => consumerGroupIds = consumerGroupIds + groupId
                  case None          => existsOffsetWithoutGroupId = true
                }

                record.committableOffset
              }

              val consumerGroupId =
                if (existsOffsetWithoutGroupId || consumerGroupIds.size != 1)
                  F.raiseError(ConsumerGroupException(consumerGroupIds))
                else F.pure(consumerGroupIds.head)

              consumerGroupId.flatMap { groupId =>
                withProducer { producer =>
                  F.bracketCase(F.delay(producer.beginTransaction())) { _ =>
                    val records: ArrayBuffer[ProducerRecord[K, V]] =
                      new ArrayBuffer(message.records.size)

                    message.records.foreach { committable =>
                      committable.foldable.foldLeft(committable.records, ()) {
                        case (_, record) =>
                          records += record
                      }
                    }

                    Chunk
                      .buffer(records)
                      .traverse(KafkaProducer.produceRecord(settings, producer))
                      .map(_.sequence)
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
                }.flatten
              }
            }
          }

          override def toString: String =
            "TransactionalKafkaProducer$" + System.identityHashCode(this)
        }
      }
}

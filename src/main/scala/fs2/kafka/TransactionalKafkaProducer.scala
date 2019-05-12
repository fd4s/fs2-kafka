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

import cats.{FlatMap, Traverse}
import cats.effect.concurrent.Deferred
import cats.effect.{ConcurrentEffect, ContextShift, ExitCase, IO, Resource}
import cats.implicits._
import fs2.kafka.internal._
import org.apache.kafka.clients.producer.{Callback, RecordMetadata}
import scala.collection.JavaConverters._

/**
  * [[TransactionalKafkaProducer]] represents a producer of Kafka messages specialized
  * for "read-process-write" streams, with the ability to simultaneously produce
  * `ProducerRecord`s and commit corresponding `CommittableOffset`s using [[produce]].
  * Records are wrapped in [[TransactionalProducerMessage]] which allow an arbitrary value, that
  * is a passthrough, to be included in the result.
  */
sealed abstract class TransactionalKafkaProducer[F[_], K, V] {

  /**
    * Produces the `ProducerRecord`s in the specified [[TransactionalProducerMessage]]
    * in four steps: first a transaction is initialized, then the records are
    * placed in the buffer of the producer, then the offsets of the records are
    * sent to the trancation, and lastly the transaction is completed. The
    * returned effect completes when the transaction is successfully completed.<br>
    * <br>
    * If you're only interested in the passthrough value, and not the whole
    * [[ProducerResult]], you can instead use [[producePassthrough]] which
    * only keeps the passthrough value in the output.
    */
  def produce[G[+ _], P](
    message: TransactionalProducerMessage[F, G, K, V, P]
  ): F[ProducerResult[G, K, V, P]]

  /**
    * Like [[produce]] but only keeps the passthrough value of the
    * [[ProducerResult]] rather than the whole [[ProducerResult]].
    */
  def producePassthrough[G[+ _], P](
    message: TransactionalProducerMessage[F, G, K, V, P]
  ): F[P]
}

private[kafka] object TransactionalKafkaProducer {
  def producerResource[F[_], K, V](
    settings: ProducerSettings[F, K, V]
  )(
    implicit F: ConcurrentEffect[F],
    context: ContextShift[F]
  ): Resource[F, TransactionalKafkaProducer[F, K, V]] =
    WithProducer
      .of(settings)
      .evalTap { withProducer =>
        withProducer { producer =>
          F.delay(producer.initTransactions())
        }
      }
      .map { withProducer =>
        new TransactionalKafkaProducer[F, K, V] {
          override def produce[G[+ _], P](
            message: TransactionalProducerMessage[F, G, K, V, P]
          ): F[ProducerResult[G, K, V, P]] =
            produceTransaction(message).map(
              ProducerResult(_, message.passthrough)(message.traverse)
            )

          override def producePassthrough[G[+ _], P](
            message: TransactionalProducerMessage[F, G, K, V, P]
          ): F[P] = produceTransaction(message).as(message.passthrough)

          private[this] def produceTransaction[G[+ _], P](
            message: TransactionalProducerMessage[F, G, K, V, P]
          ): F[G[(ProducerRecord[K, V], RecordMetadata)]] = {
            implicit val traverse: Traverse[G] = message.traverse
            implicit val flatMap: FlatMap[G] = message.flatMap

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
                  message.records
                    .flatMap(_.records)
                    .traverse(produceRecord(producer))
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

          private[this] def produceRecord(
            producer: ByteProducer
          )(
            record: ProducerRecord[K, V]
          ): F[F[(ProducerRecord[K, V], RecordMetadata)]] =
            asJavaRecord(record).flatMap { javaRecord =>
              Deferred[F, Either[Throwable, (ProducerRecord[K, V], RecordMetadata)]].flatMap {
                deferred =>
                  F.delay {
                      producer.send(
                        javaRecord,
                        callback { (metadata, throwable) =>
                          val complete =
                            deferred.complete {
                              if (throwable == null)
                                Right((record, metadata))
                              else Left(throwable)
                            }

                          F.runAsync(complete)(_ => IO.unit).unsafeRunSync()
                        }
                      )
                    }
                    .as(deferred.get.rethrow)
              }
            }

          private[this] def serializeToBytes(
            record: ProducerRecord[K, V]
          ): F[(Array[Byte], Array[Byte])] = {
            val keyBytes =
              settings.keySerializer
                .serialize(record.topic, record.headers, record.key)

            val valueBytes =
              settings.valueSerializer
                .serialize(record.topic, record.headers, record.value)

            keyBytes.product(valueBytes)
          }

          private[this] def asJavaRecord(
            record: ProducerRecord[K, V]
          ): F[KafkaProducerRecord] =
            serializeToBytes(record).map {
              case (keyBytes, valueBytes) =>
                new KafkaProducerRecord(
                  record.topic,
                  if (record.partition.isDefined)
                    record.partition.get: java.lang.Integer
                  else null,
                  if (record.timestamp.isDefined)
                    record.timestamp.get: java.lang.Long
                  else null,
                  keyBytes,
                  valueBytes,
                  record.headers.asJava
                )
            }

          private[this] def callback(f: (RecordMetadata, Throwable) => Unit): Callback =
            new Callback {
              override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit =
                f(metadata, exception)
            }

          override def toString: String =
            "TransactionalKafkaProducer$" + System.identityHashCode(this)
        }
      }
}

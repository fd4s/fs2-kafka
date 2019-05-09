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

import cats.Traverse
import cats.effect.concurrent.Deferred
import cats.effect.{ConcurrentEffect, ContextShift, ExitCase, IO, Resource, Sync}
import cats.implicits._
import fs2.kafka.internal.syntax._
import org.apache.kafka.clients.producer.{Callback, Producer, RecordMetadata}

import scala.concurrent.ExecutionContext

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
  private[this] def executionContextResource[F[_], K, V](
    settings: ProducerSettings[F, K, V]
  )(implicit F: Sync[F]): Resource[F, ExecutionContext] =
    settings.executionContext match {
      case Some(executionContext) => Resource.pure(executionContext)
      case None                   => producerExecutionContextResource
    }

  private[this] def createProducer[F[_], K, V](
    settings: ProducerSettings[F, K, V],
    executionContext: ExecutionContext
  )(
    implicit F: Sync[F],
    context: ContextShift[F]
  ): Resource[F, Producer[Array[Byte], Array[Byte]]] = {
    Resource
      .make[F, Producer[Array[Byte], Array[Byte]]] {
        settings.createProducer
      } { producer =>
        context.evalOn(executionContext) {
          F.delay(producer.close(settings.closeTimeout.asJava))
        }
      }
      .evalMap { producer =>
        context.evalOn(executionContext)(F.delay(producer.initTransactions())).as(producer)
      }
  }

  def producerResource[F[_], K, V](settings: ProducerSettings[F, K, V])(
    implicit F: ConcurrentEffect[F],
    context: ContextShift[F]
  ): Resource[F, TransactionalKafkaProducer[F, K, V]] =
    executionContextResource(settings).flatMap { executionContext =>
      createProducer(settings, executionContext).map { producer =>
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
            implicit val G: Traverse[G] = message.traverse

            F.bracketCase(context.evalOn(executionContext)(F.delay(producer.beginTransaction()))) {
              _ =>
                import scala.collection.JavaConverters._

                for {
                  waitForMetadata <- message.records
                    .traverse(record => produceRecord(record).map(_.map(record -> _)))
                    .map(_.sequence)
                  _ <- context.evalOn(executionContext)(F.delay {
                    producer.sendOffsetsToTransaction(
                      message.offsetBatch.offsets.asJava,
                      message.consumerGroupId
                    )
                  })
                  _ <- context.evalOn(executionContext)(F.delay(producer.commitTransaction()))
                  recordsWithMetadata <- waitForMetadata
                } yield {
                  recordsWithMetadata
                }
            } { (_, exitCase) =>
              exitCase match {
                case ExitCase.Completed => F.unit
                case _                  => context.evalOn(executionContext)(F.delay(producer.abortTransaction()))
              }
            }
          }

          private[this] def produceRecord(
            record: ProducerRecord[K, V]
          ): F[F[RecordMetadata]] =
            asJavaRecord(record, settings.shiftSerialization).flatMap { javaRecord =>
              Deferred[F, Either[Throwable, RecordMetadata]].flatMap { deferred =>
                F.delay {
                    producer.send(
                      javaRecord,
                      callback { (metadata, throwable) =>
                        val complete =
                          deferred.complete {
                            if (throwable == null)
                              Right(metadata)
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
            record: ProducerRecord[K, V],
            shiftSerialization: Boolean
          ): F[(Array[Byte], Array[Byte])] = {
            def serialize = {
              val keyBytes =
                settings.keySerializer
                  .serialize(record.topic, record.headers, record.key)

              val valueBytes =
                settings.valueSerializer
                  .serialize(record.topic, record.headers, record.value)

              keyBytes.product(valueBytes)
            }

            if (shiftSerialization)
              context.evalOn(executionContext)(serialize)
            else serialize
          }

          private[this] def asJavaRecord(
            record: ProducerRecord[K, V],
            shiftSerialization: Boolean
          ): F[KafkaProducerRecord] =
            serializeToBytes(record, shiftSerialization).map {
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
}

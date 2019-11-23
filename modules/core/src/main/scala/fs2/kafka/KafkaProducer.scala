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

import java.time.Instant

import cats.Apply
import cats.effect._
import cats.effect.concurrent.{Deferred, Ref}
import cats.implicits._
import fs2.Chunk
import fs2.kafka.internal._
import org.apache.kafka.clients.producer.{Callback, RecordMetadata}
import org.apache.kafka.common.TopicPartition

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

private[kafka] object KafkaProducer {
  def resource[F[_], K, V](
    settings: ProducerSettings[F, K, V]
  )(
    implicit F: ConcurrentEffect[F],
    context: ContextShift[F]
  ): Resource[F, KafkaProducer[F, K, V]] =
    Resource.liftF(settings.keySerializer).flatMap { keySerializer =>
      Resource.liftF(settings.valueSerializer).flatMap { valueSerializer =>
        WithProducer(settings).map { withProducer =>
          new KafkaProducer[F, K, V] {
            override def produce[P](
              records: ProducerRecords[K, V, P]
            ): F[F[ProducerResult[K, V, P]]] = {
              withProducer { producer =>
                records.records
                  .traverse(produceRecord(keySerializer, valueSerializer, producer))
                  .map(_.sequence.map(ProducerResult(_, records.passthrough)))
              }
            }

            override def toString: String =
              "KafkaProducer$" + System.identityHashCode(this)
          }
        }
      }
    }

  def unit[F[_], K, V](
    implicit F: Sync[F],
    context: ConcurrentEffect[F]
  ): F[KafkaProducer[F, K, V]] =
    F.pure(new KafkaProducer[F, K, V] {

      import scala.collection.mutable.Map
      import scala.collection.mutable.ListBuffer

      val kafkaBrokerRef: Ref[F, Map[String, ListBuffer[RecordMetadata]]] =
        Ref.unsafe[F, Map[String, ListBuffer[RecordMetadata]]](
          Map.empty[String, ListBuffer[RecordMetadata]]
        )

      override def produce[P](records: ProducerRecords[K, V, P]): F[F[ProducerResult[K, V, P]]] = {
        val prRec: F[Chunk[(ProducerRecord[K, V], RecordMetadata)]] = records.records.traverse {
          pr =>
            {
              kafkaBrokerRef.get.map {
                broker =>
                  {
                    val queue: ListBuffer[RecordMetadata] =
                      broker.getOrElse(pr.topic, ListBuffer.empty[RecordMetadata])
                    val timestamp = Instant.now().toEpochMilli
                    val topic = new TopicPartition(pr.topic, pr.partition.getOrElse(1))
                    queue.lastOption.fold {
                      val newElem = new RecordMetadata(topic, 1L, 0L, timestamp, 1L, 1, 1)
                      val newQueue = queue += newElem
                      broker += (pr.topic -> newQueue)
                      (pr, newElem)
                    } { le =>
                      val newElem = new RecordMetadata(topic, le.offset(), 1L, timestamp, 1L, 1, 1)
                      val newQueue = queue += newElem
                      broker += (pr.topic -> newQueue)
                      (pr, newElem)
                    }
                  }
              }
            }
        }
        val passthrough: P = records.passthrough

        F.pure(prRec.map { chunk =>
          ProducerResult(chunk, passthrough)
        })
      }
    })

  private[kafka] def produceRecord[F[_], K, V](
    keySerializer: Serializer[F, K],
    valueSerializer: Serializer[F, V],
    producer: KafkaByteProducer
  )(
    implicit F: ConcurrentEffect[F]
  ): ProducerRecord[K, V] => F[F[(ProducerRecord[K, V], RecordMetadata)]] =
    record =>
      asJavaRecord(keySerializer, valueSerializer, record).flatMap { javaRecord =>
        Deferred[F, Either[Throwable, (ProducerRecord[K, V], RecordMetadata)]].flatMap { deferred =>
          F.delay {
              producer.send(
                javaRecord,
                callback { (metadata, exception) =>
                  val complete =
                    deferred.complete {
                      if (exception == null)
                        Right((record, metadata))
                      else Left(exception)
                    }

                  F.runAsync(complete)(_ => IO.unit).unsafeRunSync()
                }
              )
            }
            .as(deferred.get.rethrow)
        }
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

  private[this] def callback(f: (RecordMetadata, Exception) => Unit): Callback =
    new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit =
        f(metadata, exception)
    }
}

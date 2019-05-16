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

import cats.Apply
import cats.effect._
import cats.effect.concurrent.Deferred
import cats.implicits._
import cats.Traverse
import fs2.kafka.internal._
import org.apache.kafka.clients.producer.{Callback, RecordMetadata}

/**
  * [[KafkaProducer]] represents a producer of Kafka messages, with the
  * ability to produce `ProducerRecord`s using [[produce]]. Records are
  * wrapped in [[ProducerMessage]] which allow an arbitrary value, that
  * is a passthrough, to be included in the result. Most often this is
  * used for keeping the [[CommittableOffset]]s, in order to commit
  * offsets, but any value can be used as passthrough value.
  */
sealed abstract class KafkaProducer[F[_], K, V] {

  /**
    * Produces the `ProducerRecord`s in the specified [[ProducerMessage]]
    * in two steps: the first effect puts the records in the buffer of the
    * producer, and the second effect waits for the records to have been
    * sent. Note that it is very slow to wait for individual records to
    * complete sending, but if you're sure that's what you want, then
    * simply `flatten` the result from this function.<br>
    * <br>
    * If you're only interested in the passthrough value, and not the whole
    * [[ProducerResult]], you can instead use [[producePassthrough]] which
    * only keeps the passthrough value in the output.
    */
  def produce[G[+ _], P](
    message: ProducerMessage[G, K, V, P]
  ): F[F[ProducerResult[G, K, V, P]]]

  /**
    * Like [[produce]] but only keeps the passthrough value of the
    * [[ProducerResult]] rather than the whole [[ProducerResult]].
    */
  def producePassthrough[G[+ _], P](
    message: ProducerMessage[G, K, V, P]
  ): F[F[P]]
}

private[kafka] object KafkaProducer {
  def resource[F[_], K, V](
    settings: ProducerSettings[F, K, V]
  )(
    implicit F: ConcurrentEffect[F],
    context: ContextShift[F]
  ): Resource[F, KafkaProducer[F, K, V]] =
    WithProducer(settings).map { withProducer =>
      new KafkaProducer[F, K, V] {
        override def produce[G[+ _], P](
          message: ProducerMessage[G, K, V, P]
        ): F[F[ProducerResult[G, K, V, P]]] = {
          implicit val G: Traverse[G] = message.traverse

          withProducer { producer =>
            message.records
              .traverse(produceRecord(settings, producer))
              .map(_.sequence.map(ProducerResult(_, message.passthrough)))
          }
        }

        override def producePassthrough[G[+ _], P](
          message: ProducerMessage[G, K, V, P]
        ): F[F[P]] = {
          implicit val G: Traverse[G] = message.traverse

          withProducer { producer =>
            message.records
              .traverse(produceRecord(settings, producer))
              .map(_.sequence_.as(message.passthrough))
          }
        }

        override def toString: String =
          "KafkaProducer$" + System.identityHashCode(this)
      }
    }

  private[kafka] def produceRecord[F[_], K, V](
    settings: ProducerSettings[F, K, V],
    producer: KafkaByteProducer
  )(
    implicit F: ConcurrentEffect[F]
  ): ProducerRecord[K, V] => F[F[(ProducerRecord[K, V], RecordMetadata)]] =
    record =>
      asJavaRecord(settings, record).flatMap { javaRecord =>
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
    settings: ProducerSettings[F, K, V],
    record: ProducerRecord[K, V]
  )(implicit F: Apply[F]): F[(Array[Byte], Array[Byte])] = {
    val keyBytes =
      settings.keySerializer
        .serialize(record.topic, record.headers, record.key)

    val valueBytes =
      settings.valueSerializer
        .serialize(record.topic, record.headers, record.value)

    keyBytes.product(valueBytes)
  }

  private[this] def asJavaRecord[F[_], K, V](
    settings: ProducerSettings[F, K, V],
    record: ProducerRecord[K, V]
  )(implicit F: Apply[F]): F[KafkaByteProducerRecord] =
    serializeToBytes(settings, record).map {
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

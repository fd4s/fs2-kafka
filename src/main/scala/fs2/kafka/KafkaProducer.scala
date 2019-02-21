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
import cats.effect._
import cats.effect.concurrent.Deferred
import cats.effect.syntax.concurrent._
import cats.implicits._
import org.apache.kafka.clients.producer.{Callback, Producer, RecordMetadata}

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
  private[this] def createProducer[F[_], K, V](
    settings: ProducerSettings[K, V]
  )(implicit F: Concurrent[F]): Resource[F, Producer[K, V]] = {
    Resource.make[F, Producer[K, V]] {
      settings.producerFactory
        .create(settings)
    } { producer =>
      F.delay {
          producer.close(
            settings.closeTimeout.length,
            settings.closeTimeout.unit
          )
        }
        .start
        .flatMap(_.join)
    }
  }

  def producerResource[F[_], K, V](settings: ProducerSettings[K, V])(
    implicit F: ConcurrentEffect[F]
  ): Resource[F, KafkaProducer[F, K, V]] =
    createProducer(settings).map { producer =>
      new KafkaProducer[F, K, V] {
        override def produce[G[+ _], P](
          message: ProducerMessage[G, K, V, P]
        ): F[F[ProducerResult[G, K, V, P]]] = {
          implicit val G: Traverse[G] =
            message.traverse

          message.records
            .traverse(record => produceRecord(record, (record, _)))
            .map(_.sequence.map(ProducerResult(_, message.passthrough)))
        }

        override def producePassthrough[G[+ _], P](
          message: ProducerMessage[G, K, V, P]
        ): F[F[P]] = {
          implicit val G: Traverse[G] =
            message.traverse

          message.records
            .traverse(record => produceRecord(record, _ => ()))
            .map(_.sequence_.as(message.passthrough))
        }

        private[this] def produceRecord[A](
          record: ProducerRecord[K, V],
          result: RecordMetadata => A
        ): F[F[A]] =
          Deferred[F, Either[Throwable, A]].flatMap { deferred =>
            F.delay {
                producer.send(
                  asJavaRecord(record),
                  callback { (metadata, throwable) =>
                    val complete =
                      deferred.complete {
                        if (throwable == null)
                          Right(result(metadata))
                        else Left(throwable)
                      }

                    F.runAsync(complete)(_ => IO.unit).unsafeRunSync()
                  }
                )
              }
              .as(deferred.get.rethrow)
          }

        private[this] def asJavaRecord(
          record: ProducerRecord[K, V]
        ): KafkaProducerRecord[K, V] =
          new KafkaProducerRecord[K, V](
            record.topic,
            if (record.partition.isDefined)
              record.partition.get: java.lang.Integer
            else null,
            if (record.timestamp.isDefined)
              record.timestamp.get: java.lang.Long
            else null,
            record.key,
            record.value,
            record.headers.asJava
          )

        private[this] def callback(f: (RecordMetadata, Throwable) => Unit): Callback =
          new Callback {
            override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit =
              f(metadata, exception)
          }

        override def toString: String =
          "KafkaProducer$" + System.identityHashCode(this)
      }
    }
}

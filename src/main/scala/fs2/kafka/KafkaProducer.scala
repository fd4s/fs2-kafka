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
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.monadError._
import cats.syntax.traverse._
import org.apache.kafka.clients.producer._

/**
  * [[KafkaProducer]] represents a producer of Kafka messages, with the
  * ability to produce `ProducerRecord`s, either using [[produce]] for
  * a one-off produce, while also waiting for the records to be sent,
  * or with [[produceBatched]] for multiple records over time.<br>
  * <br>
  * Records are wrapped in [[ProducerMessage]] which allow some arbitrary
  * passthroughs to be included in the [[ProducerResult]]s. This is mostly
  * useful for keeping the [[CommittableOffset]]s of consumed messages,
  * but any values can be used as passthrough values.
  */
sealed abstract class KafkaProducer[F[_], K, V] {

  /**
    * Produces the `ProducerRecord`s in the specified [[ProducerMessage]]
    * in two steps: first effect puts the records in the producer buffer,
    * and the second effect waits for the records to have been sent,
    * according to configured producer settings. Generally, this version
    * should be preferred over [[produce]], since it can achieve
    * significantly better performance.
    */
  def produceBatched[G[_], P](
    message: ProducerMessage[G, K, V, P]
  ): F[F[ProducerResult[G, K, V, P]]]

  /**
    * Produces the `ProducerRecord`s in the specified [[ProducerMessage]]
    * and waits for the records to have been sent, according to configured
    * producer settings. For performance reasons, instead prefer to use
    * [[produceBatched]] whenever possible.
    */
  def produce[G[_], P](
    message: ProducerMessage[G, K, V, P]
  ): F[ProducerResult[G, K, V, P]]
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

  private[this] def callback(f: (RecordMetadata, Throwable) => Unit): Callback =
    new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit =
        f(metadata, exception)
    }

  def producerResource[F[_], K, V](settings: ProducerSettings[K, V])(
    implicit F: ConcurrentEffect[F]
  ): Resource[F, KafkaProducer[F, K, V]] =
    createProducer(settings).map { producer =>
      new KafkaProducer[F, K, V] {
        override def produceBatched[G[_], P](
          message: ProducerMessage[G, K, V, P]
        ): F[F[ProducerResult[G, K, V, P]]] = {
          implicit val G: Traverse[G] = message.traverse
          message.records
            .traverse { record =>
              Deferred[F, Either[Throwable, (ProducerRecord[K, V], RecordMetadata)]]
                .flatMap { deferred =>
                  F.delay {
                      producer.send(
                        record,
                        callback {
                          case (metadata, throwable) =>
                            val result = Option(throwable).toLeft(record -> metadata)
                            val complete = deferred.complete(result)
                            F.runAsync(complete)(_ => IO.unit).unsafeRunSync()
                        }
                      )
                    }
                    .as(deferred.get.rethrow)
                }
            }
            .map(_.sequence.map(ProducerResult(_, message.passthrough)))
        }

        override def produce[G[_], P](
          message: ProducerMessage[G, K, V, P]
        ): F[ProducerResult[G, K, V, P]] =
          produceBatched(message).flatten

        override def toString: String =
          "KafkaProducer$" + System.identityHashCode(this)
      }
    }
}

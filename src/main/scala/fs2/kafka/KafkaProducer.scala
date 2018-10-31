/*
 * Copyright 2018 OVO Energy Ltd
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

import cats.Applicative
import cats.effect.concurrent.Deferred
import cats.effect.{ConcurrentEffect, IO, Resource, Sync}
import cats.instances.list._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.monadError._
import cats.syntax.traverse._
import org.apache.kafka.clients.producer.{KafkaProducer => KProducer, _}

import scala.collection.JavaConverters._

abstract class KafkaProducer[F[_], K, V] {
  def produceBatched[P](message: ProducerMessage[K, V, P]): F[F[ProducerResult[K, V, P]]]

  def produce[P](message: ProducerMessage[K, V, P]): F[ProducerResult[K, V, P]]
}

private[kafka] object KafkaProducer {
  private[this] def createProducer[F[_], K, V](
    settings: ProducerSettings[K, V]
  )(implicit F: Sync[F]): Resource[F, Producer[K, V]] = {
    Resource.make[F, Producer[K, V]] {
      F.delay {
        new KProducer(
          (settings.properties: Map[String, AnyRef]).asJava,
          settings.keySerializer,
          settings.valueSerializer
        )
      }
    } { producer =>
      F.delay {
        producer.close(
          settings.closeTimeout.length,
          settings.closeTimeout.unit
        )
      }
    }
  }

  private[this] def produceSingle[F[_], K, V, P](
    producer: Producer[K, V],
    record: ProducerRecord[K, V],
    passthrough: P
  )(implicit F: ConcurrentEffect[F]): F[F[ProducerResult[K, V, P]]] =
    Deferred[F, Either[Throwable, ProducerResult[K, V, P]]]
      .flatMap { deferred =>
        F.delay {
            producer.send(
              record,
              callback {
                case (metadata, throwable) =>
                  val result = Option(throwable).toLeft {
                    ProducerResult.single(metadata, record, passthrough)
                  }

                  val complete = deferred.complete(result)
                  F.runAsync(complete)(_ => IO.unit).unsafeRunSync()
              }
            )
          }
          .as(deferred.get.rethrow)
      }

  private[this] def produceMultiple[F[_], K, V, P](
    producer: Producer[K, V],
    records: List[ProducerRecord[K, V]],
    passthrough: P
  )(implicit F: ConcurrentEffect[F]): F[F[ProducerResult[K, V, P]]] =
    records
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
      .map { produced =>
        produced.sequence.map { records =>
          ProducerResult.multiple(
            parts = records.map {
              case (record, metadata) =>
                ProducerResult.multiplePart(
                  metadata = metadata,
                  record = record
                )
            },
            passthrough = passthrough
          )
        }
      }

  private[this] def producePassthrough[F[_], K, V, P](
    passthrough: P
  )(implicit F: Applicative[F]): F[F[ProducerResult[K, V, P]]] =
    F.pure(F.pure(ProducerResult.passthrough(passthrough)))

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
        override def produceBatched[P](
          message: ProducerMessage[K, V, P]
        ): F[F[ProducerResult[K, V, P]]] = message match {
          case ProducerMessage.Single(record, passthrough) =>
            produceSingle(producer, record, passthrough)

          case ProducerMessage.Multiple(records, passthrough) =>
            produceMultiple(producer, records, passthrough)

          case ProducerMessage.Passthrough(passthrough) =>
            producePassthrough(passthrough)
        }

        override def produce[P](
          message: ProducerMessage[K, V, P]
        ): F[ProducerResult[K, V, P]] =
          produceBatched(message).flatten

        override def toString: String =
          "KafkaProducer$" + System.identityHashCode(this)
      }
    }
}

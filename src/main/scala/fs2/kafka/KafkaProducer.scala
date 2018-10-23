package fs2.kafka

import cats.Applicative
import cats.effect.concurrent.Deferred
import cats.effect.{ConcurrentEffect, IO, Sync}
import cats.instances.list._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.monadError._
import cats.syntax.traverse._
import fs2.Stream
import org.apache.kafka.clients.producer.{KafkaProducer => KProducer, _}

import scala.collection.JavaConverters._

abstract class KafkaProducer[F[_], K, V] {
  def produceWithBatching[P](
    message: ProducerMessage[K, V, P]
  ): F[F[ProducerResult[K, V, P]]]
}

object KafkaProducer {
  private[this] def createProducer[F[_], K, V](
    settings: ProducerSettings[K, V]
  )(implicit F: Sync[F]): Stream[F, Producer[K, V]] = {
    Stream.bracket {
      F.delay {
        new KProducer(
          settings.nativeSettings.asJava,
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

  private[kafka] def producerStream[F[_], K, V](settings: ProducerSettings[K, V])(
    implicit F: ConcurrentEffect[F]
  ): Stream[F, KafkaProducer[F, K, V]] =
    createProducer(settings).map { producer =>
      new KafkaProducer[F, K, V] {
        override def produceWithBatching[P](
          message: ProducerMessage[K, V, P]
        ): F[F[ProducerResult[K, V, P]]] = message match {
          case ProducerMessage.Single(record, passthrough) =>
            produceSingle(producer, record, passthrough)

          case ProducerMessage.Multiple(records, passthrough) =>
            produceMultiple(producer, records, passthrough)

          case ProducerMessage.Passthrough(passthrough) =>
            producePassthrough(passthrough)
        }

        override def toString: String =
          "KafkaProducer$" + System.identityHashCode(this)
      }
    }
}

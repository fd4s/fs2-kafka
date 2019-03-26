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

package fs2.kafka.internal

import cats.{Foldable, Show}
import cats.effect.{CancelToken, Concurrent, Sync}
import cats.syntax.foldable._
import cats.syntax.show._
import fs2.kafka.{Header, Headers, KafkaHeaders}
import java.time.Duration
import java.time.temporal.{ChronoUnit, TemporalUnit}
import java.util
import java.util.concurrent.{CancellationException, CompletionException, TimeUnit}

import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.KafkaFuture.{BaseFunction, BiConsumer}

import scala.collection.immutable.SortedSet
import scala.concurrent.duration.FiniteDuration

private[kafka] object syntax {
  implicit final class FiniteDurationSyntax(
    private val duration: FiniteDuration
  ) extends AnyVal {
    def asJava: Duration =
      if (duration.length == 0L) Duration.ZERO
      else
        duration.unit match {
          case TimeUnit.DAYS         => Duration.ofDays(duration.length)
          case TimeUnit.HOURS        => Duration.ofHours(duration.length)
          case TimeUnit.MINUTES      => Duration.ofMinutes(duration.length)
          case TimeUnit.SECONDS      => Duration.ofSeconds(duration.length)
          case TimeUnit.MILLISECONDS => Duration.ofMillis(duration.length)
          case TimeUnit.MICROSECONDS => Duration.of(duration.length, ChronoUnit.MICROS)
          case TimeUnit.NANOSECONDS  => Duration.ofNanos(duration.length)
        }
  }

  implicit final class TimeUnitSyntax(
    private val timeUnit: TimeUnit,
  ) extends AnyVal {
    def asTemporalUnit: TemporalUnit = {
      timeUnit match {
        case TimeUnit.NANOSECONDS => ChronoUnit.NANOS
        case TimeUnit.MICROSECONDS => ChronoUnit.MICROS
        case TimeUnit.MILLISECONDS => ChronoUnit.MILLIS
        case TimeUnit.SECONDS => ChronoUnit.SECONDS
        case TimeUnit.MINUTES => ChronoUnit.MINUTES
        case TimeUnit.HOURS => ChronoUnit.HOURS
        case TimeUnit.DAYS => ChronoUnit.DAYS
      }
    }
  }

  implicit final class FoldableSyntax[F[_], A](
    private val fa: F[A]
  ) extends AnyVal {
    def mkStringAppend(f: (String => Unit, A) => Unit)(
      start: String,
      sep: String,
      end: String
    )(implicit F: Foldable[F]): String = {
      val builder = new java.lang.StringBuilder(start)
      val append: String => Unit = s => { builder.append(s); () }
      var first = true

      fa.foldLeft(()) { (_, a) =>
        if (first) first = false
        else builder.append(sep)
        f(append, a)
      }

      builder.append(end).toString
    }

    def mkStringMap(f: A => String)(start: String, sep: String, end: String)(
      implicit F: Foldable[F]
    ): String = mkStringAppend((append, a) => append(f(a)))(start, sep, end)

    def mkString(start: String, sep: String, end: String)(
      implicit F: Foldable[F]
    ): String = mkStringMap(_.toString)(start, sep, end)

    def mkStringShow(start: String, sep: String, end: String)(
      implicit F: Foldable[F],
      A: Show[A]
    ): String = mkStringMap(_.show)(start, sep, end)

    def asJava(implicit F: Foldable[F]): util.List[A] = {
      val array = new util.ArrayList[A](fa.size.toInt)
      fa.foldLeft(())((_, a) => { array.add(a); () })
      util.Collections.unmodifiableList(array)
    }
  }

  implicit final class MapSyntax[K, V](
    private val map: Map[K, V]
  ) extends AnyVal {
    def keySetStrict: Set[K] = {
      val builder = Set.newBuilder[K]
      map.foreach(builder += _._1)
      builder.result()
    }

    def filterKeysStrict(p: K => Boolean): Map[K, V] = {
      val builder = Map.newBuilder[K, V]
      map.foreach(e => if (p(e._1)) builder += e)
      builder.result()
    }

    def filterKeysStrictList(p: K => Boolean): List[(K, V)] = {
      val builder = List.newBuilder[(K, V)]
      map.foreach(e => if (p(e._1)) builder += e)
      builder.result()
    }

    def filterKeysStrictValuesList(p: K => Boolean): List[V] = {
      val builder = List.newBuilder[V]
      map.foreach(e => if (p(e._1)) builder += e._2)
      builder.result()
    }
  }

  implicit final class JavaUtilCollectionSyntax[A](
    private val collection: util.Collection[A]
  ) extends AnyVal {
    def toSet: Set[A] = {
      val builder = Set.newBuilder[A]
      val it = collection.iterator()
      while (it.hasNext) builder += it.next()
      builder.result()
    }

    def toList: List[A] = {
      val builder = List.newBuilder[A]
      val it = collection.iterator()
      while (it.hasNext) builder += it.next()
      builder.result()
    }

    def mapToList[B](f: A => B): List[B] = {
      val builder = List.newBuilder[B]
      val it = collection.iterator()
      while (it.hasNext) builder += f(it.next())
      builder.result()
    }

    def toSortedSet(implicit ordering: Ordering[A]): SortedSet[A] = {
      val builder = SortedSet.newBuilder[A]
      val it = collection.iterator()
      while (it.hasNext) builder += it.next()
      builder.result()
    }
  }

  implicit final class JavaUtilMapSyntax[K, V](
    private val map: util.Map[K, V]
  ) extends AnyVal {
    def toMap: Map[K, V] = {
      var result = Map.empty[K, V]
      val it = map.entrySet.iterator()
      while (it.hasNext) {
        val e = it.next()
        result = result.updated(e.getKey, e.getValue)
      }
      result
    }
  }

  implicit final class KafkaFutureSyntax[A](
    private val future: KafkaFuture[A]
  ) extends AnyVal {
    private[this] def baseFunction[B](f: A => B): BaseFunction[A, B] =
      new BaseFunction[A, B] { override def apply(a: A): B = f(a) }

    def map[B](f: A => B): KafkaFuture[B] =
      future.thenApply(baseFunction(f))

    def void: KafkaFuture[Unit] =
      map(_ => ())

    def cancelToken[F[_]](implicit F: Sync[F]): CancelToken[F] =
      F.delay { future.cancel(true); () }

    // Inspired by Monix's `CancelableFuture#fromJavaCompletable`.
    def cancelable[F[_]](implicit F: Concurrent[F]): F[A] =
      F.cancelable { cb =>
        future
          .whenComplete(new BiConsumer[A, Throwable] {
            override def accept(a: A, t: Throwable): Unit = t match {
              case null                                         => cb(Right(a))
              case _: CancellationException                     => ()
              case e: CompletionException if e.getCause != null => cb(Left(e.getCause))
              case e                                            => cb(Left(e))
            }
          })
          .cancelToken
      }
  }

  implicit final class KafkaHeadersSyntax(
    private val headers: KafkaHeaders
  ) extends AnyVal {
    def asScala: Headers =
      Headers.fromSeq {
        headers.toArray.map { header =>
          Header(header.key, header.value)
        }
      }
  }
}

/*
 * Copyright 2018-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.{FlatMap, Foldable, Show}
import cats.effect.Async
import cats.syntax.all._
import fs2.kafka.{Header, Headers, KafkaHeaders}
import fs2.kafka.internal.converters.unsafeWrapArray
import fs2.kafka.internal.converters.collection._

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util
import java.util.concurrent.TimeUnit
import org.apache.kafka.common.KafkaFuture

import scala.collection.immutable.SortedSet
import scala.concurrent.duration.FiniteDuration

private[kafka] object syntax {
  implicit final class LoggingSyntax[F[_], A](
    private val fa: F[A]
  ) extends AnyVal {
    def log(f: A => LogEntry)(implicit
      F: FlatMap[F],
      logging: Logging[F]
    ): F[Unit] =
      fa.flatMap(a => logging.log(f(a)))
  }

  implicit final class FiniteDurationSyntax(
    private val duration: FiniteDuration
  ) extends AnyVal {
    def toJava: Duration =
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

    def mkStringMap(f: A => String)(start: String, sep: String, end: String)(implicit
      F: Foldable[F]
    ): String = mkStringAppend((append, a) => append(f(a)))(start, sep, end)

    def mkString(start: String, sep: String, end: String)(implicit
      F: Foldable[F]
    ): String = mkStringMap(_.toString)(start, sep, end)

    def mkStringShow(start: String, sep: String, end: String)(implicit
      F: Foldable[F],
      A: Show[A]
    ): String = mkStringMap(_.show)(start, sep, end)

    def asJava(implicit F: Foldable[F]): util.List[A] = {
      val array = new util.ArrayList[A](fa.size.toInt)
      fa.foldLeft(()) { (_, a) => array.add(a); () }
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

    def updatedIfAbsent(k: K, v: => V): Map[K, V] =
      if (map.contains(k)) map else map.updated(k, v)
  }

  implicit final class MapWrappedValueSyntax[F[_], K, V](
    private val map: Map[K, F[V]]
  ) extends AnyVal {
    def asJavaMap(implicit F: Foldable[F]): util.Map[K, util.Collection[V]] =
      map.map { case (k, fv) => k -> (fv.asJava: util.Collection[V]) }.asJava
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

    def toVector: Vector[A] = {
      val builder = Vector.newBuilder[A]
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

  implicit final class KafkaFutureSyntax[F[_], A](
    private val futureF: F[KafkaFuture[A]]
  ) extends AnyVal {
    def cancelable_(implicit F: Async[F]): F[A] =
      F.fromCompletableFuture(futureF.map(_.toCompletionStage.toCompletableFuture))
  }

  implicit final class KafkaHeadersSyntax(
    private val headers: KafkaHeaders
  ) extends AnyVal {
    def asScala: Headers =
      Headers.fromSeq {
        unsafeWrapArray {
          headers.toArray.map { header =>
            Header(header.key, header.value)
          }
        }
      }
  }
}

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

package fs2.kafka.internal

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util
import java.util.concurrent.TimeUnit

import cats.syntax.foldable._
import cats.syntax.show._
import cats.{Foldable, Show}

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

    def toSortedSet(implicit ordering: Ordering[A]): SortedSet[A] = {
      val builder = SortedSet.newBuilder[A]
      val it = collection.iterator()
      while (it.hasNext) builder += it.next()
      builder.result()
    }
  }
}

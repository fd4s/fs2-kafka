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

import cats.effect.Concurrent
import cats.effect.concurrent.{Deferred, Ref}
import cats.syntax.flatMap._
import cats.syntax.functor._

/**
  * Provides synchronized access to a resource `A`, similar to that of
  * `synchronized(a) { use(a) }`, except the blocking is semantic only,
  * and no actual threads are blocked by the implementation.
  */
private[kafka] sealed abstract class Synchronized[F[_], A] {

  /**
    * Runs the specified function on the resource `A`, or waits until
    * given exclusive access to the resource, and then runs the given
    * function. Can be cancelled while waiting on exclusive access.
    */
  def use[B](f: A => F[B]): F[B]
}

private[kafka] object Synchronized {
  def apply[F[_]](implicit F: Concurrent[F]): ApplyBuilders[F] =
    new ApplyBuilders(F)

  def of[F[_], A](a: A)(implicit F: Concurrent[F]): F[Synchronized[F, A]] =
    Deferred[F, Unit].flatMap { initial =>
      initial.complete(()).flatMap { _ =>
        Ref.of[F, Deferred[F, Unit]](initial).map { ref =>
          new Synchronized[F, A] {
            override def use[B](f: A => F[B]): F[B] =
              Deferred[F, Unit].flatMap { next =>
                F.bracket(ref.getAndSet(next)) { current =>
                  current.get.flatMap(_ => f(a))
                }(_ => next.complete(()))
              }
          }
        }
      }
    }

  final class ApplyBuilders[F[_]](private val F: Concurrent[F]) extends AnyVal {
    def of[A](a: A): F[Synchronized[F, A]] =
      Synchronized.of(a)(F)
  }
}

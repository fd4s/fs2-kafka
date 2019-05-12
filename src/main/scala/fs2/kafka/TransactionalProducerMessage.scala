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

import cats.{FlatMap, Monad, Traverse}
import cats.syntax.applicative._

sealed abstract class TransactionalProducerMessage[F[_], G[+ _], +K, +V, +P] {
  def records: G[CommittableProducerRecords[F, G, K, V]]

  def passthrough: P

  def flatMap: FlatMap[G]

  def traverse: Traverse[G]
}

object TransactionalProducerMessage {
  private[this] final class TransactionalProducerMessageImpl[F[_], G[+ _], +K, +V, +P](
    override val records: G[CommittableProducerRecords[F, G, K, V]],
    override val passthrough: P,
    override val flatMap: FlatMap[G],
    override val traverse: Traverse[G]
  ) extends TransactionalProducerMessage[F, G, K, V, P] {
    override def toString: String =
      s"TransactionalProducerMessage($records, $passthrough)"
  }

  def apply[F[_], G[+ _], K, V](
    records: G[CommittableProducerRecords[F, G, K, V]]
  )(
    implicit flatMap: FlatMap[G],
    traverse: Traverse[G]
  ): TransactionalProducerMessage[F, G, K, V, Unit] =
    apply(records, ())

  def apply[F[_], G[+ _], K, V, P](
    records: G[CommittableProducerRecords[F, G, K, V]],
    passthrough: P
  )(
    implicit flatMap: FlatMap[G],
    traverse: Traverse[G]
  ): TransactionalProducerMessage[F, G, K, V, P] =
    new TransactionalProducerMessageImpl(records, passthrough, flatMap, traverse)

  def one[F[_], G[+ _], K, V, P](
    record: CommittableProducerRecords[F, G, K, V]
  )(
    implicit monad: Monad[G],
    traverse: Traverse[G]
  ): TransactionalProducerMessage[F, G, K, V, Unit] =
    one[F, G, K, V, Unit](record, ())

  def one[F[_], G[+ _], K, V, P](
    record: CommittableProducerRecords[F, G, K, V],
    passthrough: P
  )(
    implicit monad: Monad[G],
    traverse: Traverse[G]
  ): TransactionalProducerMessage[F, G, K, V, P] =
    apply[F, G, K, V, P](record.pure[G], passthrough)
}

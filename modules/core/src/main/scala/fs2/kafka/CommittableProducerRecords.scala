/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.syntax.bifoldable._
import cats.syntax.bitraverse._
import cats.syntax.foldable._
import cats.syntax.functor._
import cats.syntax.show._
import cats.syntax.eq._
import cats.syntax.traverse._
import cats.{Applicative, Bitraverse, Eq, Eval, Foldable, Show, Traverse}
import fs2.Chunk
import fs2.kafka.internal.syntax._

import scala.collection.mutable

/**
  * [[CommittableProducerRecords]] represents zero or more [[ProducerRecord]]s
  * and a [[CommittableOffset]], used by [[TransactionalKafkaProducer]] to
  * produce the records and commit the offset atomically.<br>
  * <br>
  * [[CommittableProducerRecords]]s can be created using one of the following options.<br>
  * <br>
  * - `CommittableProducerRecords#apply` to produce zero or more records
  * within the same transaction as the offset is committed.<br>
  * - `CommittableProducerRecords#one` to produce exactly one record within
  * the same transaction as the offset is committed.
  */
sealed abstract class CommittableProducerRecords[F[_], +K, +V] {

  /** The records to produce. Can be empty to simply commit the offset. */
  def records: Chunk[ProducerRecord[K, V]]

  /** The offset to commit. */
  def offset: CommittableOffset[F]
}

object CommittableProducerRecords {
  private[this] final class CommittableProducerRecordsImpl[F[_], +K, +V](
    override val records: Chunk[ProducerRecord[K, V]],
    override val offset: CommittableOffset[F]
  ) extends CommittableProducerRecords[F, K, V] {
    override def toString: String =
      if (records.isEmpty) s"CommittableProducerRecords(<empty>, $offset)"
      else records.mkString("CommittableProducerRecords(", ", ", s", $offset)")
  }

  /**
    * Creates a new [[CommittableProducerRecords]] for producing zero or
    * more [[ProducerRecord]]s and committing an offset atomically within
    * a transaction.
    */
  def apply[F[_], G[+_], K, V](
    records: G[ProducerRecord[K, V]],
    offset: CommittableOffset[F]
  )(implicit G: Foldable[G]): CommittableProducerRecords[F, K, V] = {
    val numRecords = G.size(records).toInt
    val chunk = if (numRecords <= 1) {
      G.get(records)(0) match {
        case None         => Chunk.empty[ProducerRecord[K, V]]
        case Some(record) => Chunk.singleton(record)
      }
    } else {
      val buf = new mutable.ArrayBuffer[ProducerRecord[K, V]](numRecords)
      G.foldLeft(records, ()) {
        case (_, record) =>
          buf += record
          ()
      }
      Chunk.buffer(buf)
    }

    new CommittableProducerRecordsImpl(chunk, offset)
  }

  /**
    * Creates a new [[CommittableProducerRecords]] for producing exactly
    * one [[ProducerRecord]] and committing an offset atomically within
    * a transaction.
    */
  def one[F[_], K, V](
    record: ProducerRecord[K, V],
    offset: CommittableOffset[F]
  ): CommittableProducerRecords[F, K, V] =
    new CommittableProducerRecordsImpl(Chunk.singleton(record), offset)

  implicit def committableProducerRecordsShow[F[_], K, V](
    implicit
    K: Show[K],
    V: Show[V]
  ): Show[CommittableProducerRecords[F, K, V]] =
    Show.show { committable =>
      if (committable.records.isEmpty)
        show"CommittableProducerRecords(<empty>, ${committable.offset})"
      else
        committable.records.mkStringShow(
          "CommittableProducerRecords(",
          ", ",
          s", ${committable.offset})"
        )
    }

  implicit def committableProducerRecordsEq[F[_], K: Eq, V: Eq]
    : Eq[CommittableProducerRecords[F, K, V]] =
    Eq.instance {
      case (l, r) =>
        l.records === r.records && l.offset === r.offset
    }

  implicit def committableProducerRecordsBitraverse[F[_]]
    : Bitraverse[CommittableProducerRecords[F, *, *]] =
    new Bitraverse[CommittableProducerRecords[F, *, *]] {
      override def bitraverse[G[_], A, B, C, D](
        fab: CommittableProducerRecords[F, A, B]
      )(f: A => G[C], g: B => G[D])(
        implicit G: Applicative[G]
      ): G[CommittableProducerRecords[F, C, D]] =
        fab.records
          .traverse { record =>
            record.bitraverse(f, g)
          }
          .map { (cd: Chunk[ProducerRecord[C, D]]) =>
            CommittableProducerRecords(cd, fab.offset)
          }

      override def bifoldLeft[A, B, C](
        fab: CommittableProducerRecords[F, A, B],
        c: C
      )(f: (C, A) => C, g: (C, B) => C): C =
        fab.records.foldLeft(c) {
          case (acc, record) =>
            record.bifoldLeft(acc)(f, g)
        }

      override def bifoldRight[A, B, C](
        fab: CommittableProducerRecords[F, A, B],
        c: Eval[C]
      )(f: (A, Eval[C]) => Eval[C], g: (B, Eval[C]) => Eval[C]): Eval[C] =
        fab.records.foldRight(c) {
          case (record, acc) =>
            record.bifoldRight(acc)(f, g)
        }
    }

  implicit def committableProducerRecordsTraverse[F[_], K]
    : Traverse[CommittableProducerRecords[F, K, *]] =
    new Traverse[CommittableProducerRecords[F, K, *]] {
      override def traverse[G[_], A, B](
        fa: CommittableProducerRecords[F, K, A]
      )(f: A => G[B])(implicit G: Applicative[G]): G[CommittableProducerRecords[F, K, B]] =
        fa.records
          .traverse { record =>
            record.traverse(f)
          }
          .map { (b: Chunk[ProducerRecord[K, B]]) =>
            CommittableProducerRecords(b, fa.offset)
          }

      override def foldLeft[A, B](fa: CommittableProducerRecords[F, K, A], b: B)(
        f: (B, A) => B
      ): B =
        fa.records.foldLeft(b) {
          case (acc, record) =>
            record.foldLeft(acc)(f)
        }

      override def foldRight[A, B](
        fa: CommittableProducerRecords[F, K, A],
        lb: Eval[B]
      )(f: (A, Eval[B]) => Eval[B]): Eval[B] =
        fa.records.foldRight(lb) {
          case (record, acc) =>
            record.foldRight(acc)(f)
        }
    }
}

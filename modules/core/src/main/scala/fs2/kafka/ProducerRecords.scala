/*
 * Copyright 2018-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.{Foldable, Show, Traverse}
import cats.syntax.show._
import fs2.Chunk
import fs2.kafka.internal.syntax._

/**
  * [[ProducerRecords]] represents zero or more `ProducerRecord`s,
  * together with an arbitrary passthrough value, all of which can
  * be used with [[KafkaProducer]]. [[ProducerRecords]]s can be
  * created using one of the following options.<br>
  * <br>
  * - `ProducerRecords#apply` to produce zero or more records
  * and then emit a [[ProducerResult]] with the results and
  * specified passthrough value.<br>
  * - `ProducerRecords#one` to produce exactly one record and
  * then emit a [[ProducerResult]] with the result and specified
  * passthrough value.<br>
  * <br>
  * The [[passthrough]] and [[records]] can be retrieved from an
  * existing [[ProducerRecords]] instance.<br>
  */
sealed abstract class ProducerRecords[+P, +K, +V] {
  /** The records to produce. Can be empty for passthrough-only. */
  def records: Chunk[ProducerRecord[K, V]]

  /** The passthrough to emit once all [[records]] have been produced. */
  def passthrough: P
}

object ProducerRecords {
  private[this] final class ProducerRecordsImpl[+P, +K, +V](
    override val records: Chunk[ProducerRecord[K, V]],
    override val passthrough: P
  ) extends ProducerRecords[P, K, V] {
    override def toString: String =
      if (records.isEmpty) s"ProducerRecords(<empty>, $passthrough)"
      else records.mkString("ProducerRecords(", ", ", s", $passthrough)")
  }

  /**
    * Creates a new [[ProducerRecords]] for producing zero or more
    * `ProducerRecords`s, then emitting a [[ProducerResult]] with
    * the results and `Unit` passthrough value.
    *
    * @see [[fs2.kafka.ProducerRecords#chunk(fs2.Chunk)]] if your `records` are already contained in an [[fs2.Chunk]]
    */
  def apply[F[+_], K, V](
    records: F[ProducerRecord[K, V]]
  )(
    implicit F: Traverse[F]
  ): ProducerRecords[Unit, K, V] =
    apply(records, ())

  /**
    * Creates a new [[ProducerRecords]] for producing zero or more
    * `ProducerRecords`s, then emitting a [[ProducerResult]] with
    * the results and specified passthrough value.
    *
    * @see [[fs2.kafka.ProducerRecords#chunk(fs2.Chunk, java.lang.Object)]] if your `records` are already contained in
    * an [[fs2.Chunk]]
    */
  def apply[F[+_], P, K, V](
    records: F[ProducerRecord[K, V]],
    passthrough: P
  )(
    implicit F: Traverse[F]
  ): ProducerRecords[P, K, V] =
    chunk(Chunk.iterable(Foldable[F].toIterable(records)), passthrough)

  /**
    * Creates a new [[ProducerRecords]] for producing exactly one
    * `ProducerRecord`, then emitting a [[ProducerResult]] with
    * the result and `Unit` passthrough value.
    */
  def one[K, V](
    record: ProducerRecord[K, V]
  ): ProducerRecords[Unit, K, V] =
    one(record, ())

  /**
    * Creates a new [[ProducerRecords]] for producing exactly one
    * `ProducerRecord`, then emitting a [[ProducerResult]] with
    * the result and specified passthrough value.
    */
  def one[P, K, V](
    record: ProducerRecord[K, V],
    passthrough: P
  ): ProducerRecords[P, K, V] =
    apply(Chunk.singleton(record), passthrough)

  /**
    * Creates a new [[ProducerRecords]] for producing zero or more
    * `ProducerRecords`s, then emitting a [[ProducerResult]] with
    * the results and `Unit` passthrough value.
    */
  def chunk[K, V](
    records: Chunk[ProducerRecord[K, V]]
  ): ProducerRecords[Unit, K, V] =
    chunk(records, ())

  /**
    * Creates a new [[ProducerRecords]] for producing zero or more
    * `ProducerRecords`s, then emitting a [[ProducerResult]] with
    * the results and specified passthrough value.
    */
  def chunk[P, K, V](
    records: Chunk[ProducerRecord[K, V]],
    passthrough: P
  ): ProducerRecords[P, K, V] =
    new ProducerRecordsImpl(records, passthrough)

  implicit def producerRecordsShow[P, K, V](
    implicit
    K: Show[K],
    V: Show[V],
    P: Show[P]
  ): Show[ProducerRecords[P, K, V]] = Show.show { records =>
    if (records.records.isEmpty) show"ProducerRecords(<empty>, ${records.passthrough})"
    else records.records.mkStringShow("ProducerRecords(", ", ", s", ${records.passthrough})")
  }
}

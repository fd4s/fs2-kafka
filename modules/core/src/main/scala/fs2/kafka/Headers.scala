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

import cats.data.Chain
import cats.Show
import fs2.kafka.internal.converters.collection._
import fs2.kafka.internal.syntax._

/**
  * [[Headers]] represent an immutable append-only collection
  * of [[Header]]s. To create a new [[Headers]] instance, you
  * can use [[Headers#apply]] or [[Headers#empty]] and add an
  * instance of [[Header]] using `append`.
  */
sealed abstract class Headers {

  /**
    * Returns the first header with the specified key,
    * wrapped in `Some`, or `None` if no such header
    * exists. Alias for [[withKey]].
    */
  final def apply(key: String): Option[Header] =
    withKey(key)

  /**
    * Returns the first header with the specified key,
    * wrapped in `Some`, or `None` if no such header
    * exists. The [[apply]] function is an alias.
    */
  def withKey(key: String): Option[Header]

  /**
    * Creates a new [[Headers]] instance with the specified
    * [[Header]] included.
    */
  def append(header: Header): Headers

  /**
    * Creates a new [[Headers]] instance including a
    * [[Header]] with the specified key and value.
    */
  def append[V](key: String, value: V)(
    implicit serializer: HeaderSerializer[V]
  ): Headers

  /**
    * Returns `true` if a header with the specified key
    * exists; otherwise `false`.
    */
  def exists(key: String): Boolean

  /**
    * Appends the specified [[Headers]] after these headers.
    */
  def concat(that: Headers): Headers

  /** The included [[Header]]s as a `Chain`. */
  def toChain: Chain[Header]

  /** The included [[Header]]s as a `Map`, where the keys is are [[Header.key]] */
  def toMap: Map[String, Header]

  /** `true` if at least one [[Header]] is included; otherwise `false`. */
  final def nonEmpty: Boolean = !isEmpty

  /** `true` if no [[Header]]s are included; otherwise `false`. */
  def isEmpty: Boolean

  /** The [[Headers]] as an immutable Java Kafka `Headers` instance. */
  def asJava: KafkaHeaders
}

object Headers {
  private[this] final class HeadersImpl(
    val headers: Map[String, Header]
  ) extends Headers {
    override def withKey(key: String): Option[Header] =
      headers.get(key)

    override def append(header: Header): Headers =
      new HeadersImpl(headers.updated(header.key, header))

    override def append[V](key: String, value: V)(
      implicit serializer: HeaderSerializer[V]
    ): Headers = append(Header(key, value))

    override def exists(key: String): Boolean =
      headers.get(key).isDefined

    override def concat(that: Headers): Headers = {
      if (that.isEmpty) this
      else new HeadersImpl(headers ++ that.toMap)
    }

    override def toChain: Chain[Header] =
      Chain.fromSeq(headers.values.toSeq)

    override val toMap: Map[String, Header] =
      headers

    override val isEmpty: Boolean =
      false

    override def asJava: KafkaHeaders = {
      val map: Map[String, Header] = toMap

      val array: Array[KafkaHeader] =
        map.values.toArray

      new KafkaHeaders {
        override def add(header: KafkaHeader): KafkaHeaders =
          throw new IllegalStateException("Headers#asJava is immutable")

        override def add(key: String, value: Array[Byte]): KafkaHeaders =
          throw new IllegalStateException("Headers#asJava is immutable")

        override def remove(key: String): KafkaHeaders =
          throw new IllegalStateException("Headers#asJava is immutable")

        override def lastHeader(key: String): KafkaHeader = {
          val index = array.lastIndexWhere(_.key == key)
          if (index != -1) array(index) else null
        }

        override def headers(key: String): java.lang.Iterable[KafkaHeader] = {
          map.get(key) match {
            case Some(value) => Iterable.apply(value: KafkaHeader).asJava
            case None => Iterable.empty[KafkaHeader].asJava
          }
        }

        override val toArray: Array[KafkaHeader] =
          array

        override def iterator(): java.util.Iterator[KafkaHeader] =
          array.iterator.asJava
      }
    }

    override def toString: String = {
      val builder = new java.lang.StringBuilder
      builder.append("Headers(")
      val iter = headers.values.iterator
      while (iter.hasNext) {
        val header = iter.next()
        builder.append(header.key)
        builder.append(" -> ")
        builder.append(java.util.Arrays.toString(header.value))
        if (iter.hasNext) builder.append(", ")
      }
      builder.append(")")
      builder.toString
    }
  }

  /**
    * Creates a new [[Headers]] instance from the specified
    * [[Header]]s.
    */
  def apply(headers: Header*): Headers =
    fromIterable(headers)

  /**
    * Creates a new [[Headers]] instance from the specified
    * `Chain` of [[Header]]s.
    */
  def fromChain(headers: Chain[Header]): Headers =
    if (headers.isEmpty) empty
    else {
      val builder = Map.newBuilder[String, Header]
      builder ++= headers.map(h => (h.key, h)).iterator
      new HeadersImpl(builder.result())
    }

  /**
    * Creates a new [[Headers]] instance from the specified
    * `Seq` of [[Header]]s.
    */
  def fromSeq(headers: Seq[Header]): Headers =
    fromIterable(headers)

  /**
    * Creates a new [[Headers]] instance from the specified
    * `Iterable` of [[Header]]s.
    */
  def fromIterable(headers: Iterable[Header]): Headers =
    if (headers.isEmpty) empty
    else {
      val builder = Map.newBuilder[String, Header]
      val iter = headers.iterator
      while (iter.hasNext) {
        val header = iter.next()
        builder.+=((header.key, header))
      }
      new HeadersImpl(builder.result())
    }

  /** The empty [[Headers]] instance without any [[Header]]s. */
  val empty: Headers =
    new Headers {
      override def withKey(key: String): Option[Header] =
        None

      override def append(header: Header): Headers =
        new HeadersImpl(Map(header.key -> header))

      override def append[V](key: String, value: V)(
        implicit serializer: HeaderSerializer[V]
      ): Headers = append(Header(key, value))

      override def exists(key: String): Boolean =
        false

      override def concat(that: Headers): Headers =
        that

      override val toChain: Chain[Header] =
        Chain.empty

      override val toMap: Map[String, Header] =
        Map.empty

      override val isEmpty: Boolean =
        true

      override val asJava: KafkaHeaders =
        new KafkaHeaders {
          override def add(header: KafkaHeader): KafkaHeaders =
            throw new IllegalStateException("Headers#asJava is immutable")

          override def add(key: String, value: Array[Byte]): KafkaHeaders =
            throw new IllegalStateException("Headers#asJava is immutable")

          override def remove(key: String): KafkaHeaders =
            throw new IllegalStateException("Headers#asJava is immutable")

          override def lastHeader(key: String): KafkaHeader =
            null

          private[this] val emptyIterator: java.util.Iterator[KafkaHeader] =
            new java.util.Iterator[KafkaHeader] {
              override val hasNext: Boolean =
                false

              override def next(): KafkaHeader =
                throw new NoSuchElementException()
            }

          private[this] val emptyIterable: java.lang.Iterable[KafkaHeader] =
            new java.lang.Iterable[KafkaHeader] {
              override val iterator: java.util.Iterator[KafkaHeader] =
                emptyIterator
            }

          override def headers(key: String): java.lang.Iterable[KafkaHeader] =
            emptyIterable

          override val toArray: Array[KafkaHeader] =
            Array.empty

          override val iterator: java.util.Iterator[KafkaHeader] =
            emptyIterator
        }

      override def toString: String =
        "Headers()"
    }

  implicit val headersShow: Show[Headers] =
    Show.fromToString
}

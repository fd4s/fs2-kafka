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

import cats.Show

/**
  * [[Header]] represents a `String` key and `Array[Byte]` value
  * which can be included as part of [[Headers]] when creating a
  * [[ProducerRecord]]. [[Headers]] are included together with a
  * record once produced, and can be used by consumers.<br>
  * <br>
  * To create a new [[Header]], use [[Header#apply]].
  */
sealed abstract class Header extends org.apache.kafka.common.header.Header {

  /** The header key. */
  override def key: String

  /** The serialized header value. */
  override def value: Array[Byte]

  /** Creates a new [[Headers]] instance with this header included. */
  final def headers: Headers =
    Headers(this)

  /** Deserializes the [[value]] to the specified type. */
  final def as[A](implicit deserializer: HeaderDeserializer[A]): A =
    deserializer.deserialize(value)

  /** Attempts to deserialize the [[value]] to the specified type. */
  final def attemptAs[A](
    implicit deserializer: HeaderDeserializer[Either[Throwable, A]]
  ): Either[Throwable, A] =
    deserializer.deserialize(value)
}

object Header {
  private[this] final class HeaderImpl(
    override val key: String,
    override val value: Array[Byte]
  ) extends Header {
    override def toString: String =
      s"Header($key -> ${java.util.Arrays.toString(value)})"
  }

  private[this] def create(
    key: String,
    value: Array[Byte]
  ): Header =
    new HeaderImpl(
      key = key,
      value = value
    )

  /**
    * Creates a new [[Header]] instance using the specified
    * `String` key and serialized `Array[Byte]` header value.
    */
  def apply(
    key: String,
    value: Array[Byte]
  ): Header = create(
    key = key,
    value = value
  )

  /**
    * Creates a new [[Header]] instance using the specified
    * `String` key and value, where the value is serialized
    * using an implicit serializer instance.
    */
  def serialize[A](
    key: String,
    value: A
  )(implicit serializer: HeaderSerializer[A]): Header =
    create(
      key = key,
      value = serializer.serialize(value)
    )

  implicit val headerShow: Show[Header] =
    Show.fromToString
}

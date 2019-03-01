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

import cats.Contravariant
import fs2.kafka.internal.syntax._
import java.nio.charset.{Charset, StandardCharsets}
import java.util.UUID
import org.apache.kafka.common.utils.Bytes

/**
  * [[Serializer]] is a functional Kafka serializer which directly
  * extends the Kafka `Serializer` interface, but doesn't make use
  * of `close` or `configure`. There is only a single function for
  * serialization, which provides access to the record headers.
  */
sealed abstract class Serializer[A] extends KafkaSerializer[A] {

  /**
    * Serializes the specified value of type `A` into bytes. The
    * Kafka topic name, to which the serialized bytes are going
    * to be sent, and record headers are available.
    */
  def serialize(topic: String, headers: Headers, a: A): Array[Byte]

  /**
    * Creates a new [[Serializer]] which applies the specified
    * function `f` on a value of type `B`, and then serializes
    * the result with this [[Serializer]].
    */
  final def contramap[B](f: B => A): Serializer[B] =
    Serializer.instance { (topic, headers, b) =>
      serialize(topic, headers, f(b))
    }

  /**
    * Creates a new [[Serializer]] which applies the specified
    * function `f` on the output bytes of this [[Serializer]].
    */
  final def mapBytes(f: Array[Byte] => Array[Byte]): Serializer[A] =
    Serializer.instance { (topic, headers, a) =>
      f(serialize(topic, headers, a))
    }

  /**
    * Creates a new [[Serializer]] which serializes `Some` values
    * using this [[Serializer]], and serializes `None` as `null`.
    */
  final def option: Serializer[Option[A]] =
    Serializer.instance {
      case (topic, headers, Some(a)) => serialize(topic, headers, a)
      case (_, _, None)              => null
    }

  /** For interoperability with Kafka serialization. */
  final override def serialize(topic: String, a: A): Array[Byte] =
    serialize(topic, Headers.empty, a)

  /** For interoperability with Kafka serialization. */
  final override def serialize(topic: String, headers: KafkaHeaders, a: A): Array[Byte] =
    serialize(topic, headers.asScala, a)

  /** Always only returns `Unit`. For interoperability with Kafka serialization. */
  final override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = ()

  /** Always only returns `Unit`. For interoperability with Kafka serialization. */
  final override def close(): Unit = ()
}

object Serializer {
  def apply[A](implicit serializer: Serializer[A]): Serializer[A] = serializer

  /**
    * Creates a new [[Serializer]] which serializes
    * all values of type `A` as `null`.
    */
  def asNull[A]: Serializer[A] =
    Serializer.const(null)

  /**
    * Creates a new [[Serializer]] which serializes
    * all values of type `A` to the specified `bytes`.
    */
  def const[A](bytes: Array[Byte]): Serializer[A] =
    Serializer.lift(_ => bytes)

  /**
    * Creates a new [[Serializer]] which delegates serialization
    * to the specified Kafka `Serializer`. Note the `close` and
    * `configure` functions won't be called for the delegate.
    */
  def delegate[A](serializer: KafkaSerializer[A]): Serializer[A] =
    Serializer.instance { (topic, headers, a) =>
      serializer.serialize(topic, headers.asJava, a)
    }

  /**
    * Creates a new [[Serializer]] which serializes all
    * values of type `A` as the empty `Array[Byte]`.
    */
  def empty[A]: Serializer[A] =
    Serializer.const(Array.emptyByteArray)

  /**
    * Creates a new [[Serializer]] which can use different
    * [[Serializer]]s depending on the record headers.
    */
  def headers[A](f: Headers => Serializer[A]): Serializer[A] =
    Serializer.instance { (topic, headers, a) =>
      f(headers).serialize(topic, headers, a)
    }

  /**
    * Creates a new [[Serializer]] from the specified function.
    * Use [[lift]] instead if the serializer doesn't need
    * access to the Kafka topic name or record headers.
    */
  def instance[A](f: (String, Headers, A) => Array[Byte]): Serializer[A] =
    new Serializer[A] {
      override def serialize(topic: String, headers: Headers, a: A): Array[Byte] =
        f(topic, headers, a)

      override def toString: String =
        "Serializer$" + System.identityHashCode(this)
    }

  /**
    * Creates a new [[Serializer]] from the specified function,
    * ignoring to which Kafka topic the bytes are going to be
    * sent and any record headers. Use [[instance]] instead
    * if the serializer needs access to the Kafka topic
    * name or the record headers.
    */
  def lift[A](f: A => Array[Byte]): Serializer[A] =
    Serializer.instance((_, _, a) => f(a))

  /**
    * Creates a new [[Serializer]] which can use different
    * [[Serializer]]s depending on the Kafka topic name to
    * which the bytes are going to be sent.
    */
  def topic[A](f: String => Serializer[A]): Serializer[A] =
    Serializer.instance { (topic, headers, a) =>
      f(topic).serialize(topic, headers, a)
    }

  /**
    * Creates a new [[Serializer]] which serializes `String`
    * values using the specified `Charset`. Note that the
    * default `String` serializer uses `UTF-8`.
    */
  def string(charset: Charset): Serializer[String] =
    Serializer.lift(_.getBytes(charset))

  /**
    * Creates a new [[Serializer]] which serializes `UUID` values
    * as `String`s with the specified `Charset`. Note that the
    * default `UUID` serializer uses `UTF-8.`
    */
  def uuid(charset: Charset): Serializer[UUID] =
    Serializer.string(charset).contramap(_.toString)

  /**
    * The identity [[Serializer]], which does not perform any kind
    * of serialization, simply using the input bytes as the output.
    */
  implicit val identity: Serializer[Array[Byte]] =
    Serializer.lift(bytes => bytes)

  /**
    * The option [[Serializer]] serializes `None` as `null`, and
    * serializes `Some` values using the serializer for type `A`.
    */
  implicit def option[A](
    implicit serializer: Serializer[A]
  ): Serializer[Option[A]] =
    serializer.option

  implicit val contravariant: Contravariant[Serializer] =
    new Contravariant[Serializer] {
      override def contramap[A, B](serializer: Serializer[A])(f: B => A): Serializer[B] =
        serializer.contramap(f)
    }

  implicit val bytes: Serializer[Bytes] =
    Serializer.identity.contramap(_.get)

  implicit val double: Serializer[Double] =
    Serializer.delegate {
      (new org.apache.kafka.common.serialization.DoubleSerializer)
        .asInstanceOf[org.apache.kafka.common.serialization.Serializer[Double]]
    }

  implicit val float: Serializer[Float] =
    Serializer.delegate {
      (new org.apache.kafka.common.serialization.FloatSerializer)
        .asInstanceOf[org.apache.kafka.common.serialization.Serializer[Float]]
    }

  implicit val int: Serializer[Int] =
    Serializer.delegate {
      (new org.apache.kafka.common.serialization.IntegerSerializer)
        .asInstanceOf[org.apache.kafka.common.serialization.Serializer[Int]]
    }

  implicit val long: Serializer[Long] =
    Serializer.delegate {
      (new org.apache.kafka.common.serialization.LongSerializer)
        .asInstanceOf[org.apache.kafka.common.serialization.Serializer[Long]]
    }

  implicit val short: Serializer[Short] =
    Serializer.delegate {
      (new org.apache.kafka.common.serialization.ShortSerializer)
        .asInstanceOf[org.apache.kafka.common.serialization.Serializer[Short]]
    }

  implicit val string: Serializer[String] =
    Serializer.string(StandardCharsets.UTF_8)

  implicit val unit: Serializer[Unit] =
    Serializer.const(null)

  implicit val uuid: Serializer[UUID] =
    Serializer.string.contramap(_.toString)
}

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

import cats.{Applicative, ApplicativeError, Contravariant, Defer, Functor}
import cats.effect.Sync
import cats.syntax.functor._
import java.nio.charset.{Charset, StandardCharsets}
import java.util.UUID
import org.apache.kafka.common.utils.Bytes

/**
  * Kafka serializer with support for serialization effects by
  * returning serialized bytes in an abstract context `F[_]`,
  * which can include effects.
  */
sealed abstract class Serializer[F[_], A] {

  /**
    * Serializes the specified value of type `A` into bytes. The
    * Kafka topic name, to which the serialized bytes are going
    * to be sent, and record headers are available.
    */
  def serialize(topic: String, headers: Headers, a: A): F[Array[Byte]]

  /**
    * Creates a new [[Serializer]] which applies the specified
    * function `f` on a value of type `B`, and then serializes
    * the result with this [[Serializer]].
    */
  final def contramap[B](f: B => A): Serializer[F, B] =
    Serializer.instance { (topic, headers, b) =>
      serialize(topic, headers, f(b))
    }

  /**
    * Creates a new [[Serializer]] which applies the specified
    * function `f` on the output bytes of this [[Serializer]].
    */
  final def mapBytes(f: Array[Byte] => Array[Byte])(implicit F: Functor[F]): Serializer[F, A] =
    Serializer.instance { (topic, headers, a) =>
      serialize(topic, headers, a).map(f)
    }

  /**
    * Creates a new [[Serializer]] which serializes `Some` values
    * using this [[Serializer]], and serializes `None` as `null`.
    */
  final def option(implicit F: Applicative[F]): Serializer[F, Option[A]] =
    Serializer.instance {
      case (topic, headers, Some(a)) => serialize(topic, headers, a)
      case (_, _, None)              => F.pure(null)
    }

  /**
    * Creates a new [[Serializer]] which defers serialization.
    */
  final def defer(implicit F: Defer[F]): Serializer[F, A] =
    Serializer.instance { (topic, headers, a) =>
      F.defer(serialize(topic, headers, a))
    }

  /**
    * Creates a new [[Serializer]] which suspends serialization,
    * capturing any impure behaviours of this [[Serializer]].
    */
  final def suspend(implicit F: Sync[F]): Serializer[F, A] =
    Serializer.instance { (topic, headers, a) =>
      F.suspend(serialize(topic, headers, a))
    }
}

object Serializer {
  def apply[F[_], A](implicit serializer: Serializer[F, A]): Serializer[F, A] = serializer

  /**
    * Creates a new [[Serializer]] which serializes
    * all values of type `A` as `null`.
    */
  def asNull[F[_], A](implicit F: Applicative[F]): Serializer[F, A] =
    Serializer.const(null)

  /**
    * Creates a new [[Serializer]] which serializes
    * all values of type `A` to the specified `bytes`.
    */
  def const[F[_], A](bytes: Array[Byte])(implicit F: Applicative[F]): Serializer[F, A] =
    Serializer.lift(_ => F.pure(bytes))

  /**
    * Creates a new [[Serializer]] which delegates serialization
    * to the specified Kafka `Serializer`. Note the `close` and
    * `configure` functions won't be called for the delegate.<br>
    * <br>
    * It is assumed the delegate `serialize` function is pure.
    * If it's not pure, then use `suspend` after `delegate`,
    * so the impure behaviours can be captured properly.
    */
  def delegate[F[_], A](serializer: KafkaSerializer[A])(
    implicit F: Applicative[F]
  ): Serializer[F, A] =
    Serializer.instance[F, A] { (topic, headers, a) =>
      F.pure(serializer.serialize(topic, headers.asJava, a))
    }

  /**
    * Creates a new [[Serializer]] which always fails
    * serialization with the specified exception `e`.
    */
  def fail[F[_], A](e: Throwable)(
    implicit F: ApplicativeError[F, Throwable]
  ): Serializer[F, A] =
    Serializer.lift(_ => F.raiseError(e))

  /**
    * Creates a new [[Serializer]] which serializes all
    * values of type `A` as the empty `Array[Byte]`.
    */
  def empty[F[_], A](implicit F: Applicative[F]): Serializer[F, A] =
    Serializer.const(Array.emptyByteArray)

  /**
    * Creates a new [[Serializer]] which can use different
    * [[Serializer]]s depending on the record headers.
    */
  def headers[F[_], A](f: Headers => Serializer[F, A]): Serializer[F, A] =
    Serializer.instance { (topic, headers, a) =>
      f(headers).serialize(topic, headers, a)
    }

  /**
    * Creates a new [[Serializer]] from the specified function.
    * Use [[lift]] instead if the serializer doesn't need
    * access to the Kafka topic name or record headers.
    */
  def instance[F[_], A](f: (String, Headers, A) => F[Array[Byte]]): Serializer[F, A] =
    new Serializer[F, A] {
      override def serialize(topic: String, headers: Headers, a: A): F[Array[Byte]] =
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
  def lift[F[_], A](f: A => F[Array[Byte]]): Serializer[F, A] =
    Serializer.instance((_, _, a) => f(a))

  /**
    * Creates a new [[Serializer]] which can use different
    * [[Serializer]]s depending on the Kafka topic name to
    * which the bytes are going to be sent.
    */
  def topic[F[_], A](f: String => Serializer[F, A]): Serializer[F, A] =
    Serializer.instance { (topic, headers, a) =>
      f(topic).serialize(topic, headers, a)
    }

  /**
    * Creates a new [[Serializer]] which serializes `String`
    * values using the specified `Charset`. Note that the
    * default `String` serializer uses `UTF-8`.
    */
  def string[F[_]](charset: Charset)(implicit F: Applicative[F]): Serializer[F, String] =
    Serializer.lift(s => F.pure(s.getBytes(charset)))

  /**
    * Creates a new [[Serializer]] which serializes `UUID` values
    * as `String`s with the specified `Charset`. Note that the
    * default `UUID` serializer uses `UTF-8.`
    */
  def uuid[F[_]](charset: Charset)(implicit F: Applicative[F]): Serializer[F, UUID] =
    Serializer.string(charset).contramap(_.toString)

  /**
    * The identity [[Serializer]], which does not perform any kind
    * of serialization, simply using the input bytes as the output.
    */
  implicit def identity[F[_]](implicit F: Applicative[F]): Serializer[F, Array[Byte]] =
    Serializer.lift(bytes => F.pure(bytes))

  /**
    * The option [[Serializer]] serializes `None` as `null`, and
    * serializes `Some` values using the serializer for type `A`.
    */
  implicit def option[F[_], A](
    implicit F: Applicative[F],
    serializer: Serializer[F, A]
  ): Serializer[F, Option[A]] =
    serializer.option

  implicit def contravariant[F[_]]: Contravariant[Serializer[F, ?]] =
    new Contravariant[Serializer[F, ?]] {
      override def contramap[A, B](serializer: Serializer[F, A])(f: B => A): Serializer[F, B] =
        serializer.contramap(f)
    }

  implicit def bytes[F[_]](implicit F: Applicative[F]): Serializer[F, Bytes] =
    Serializer.identity.contramap(_.get)

  implicit def double[F[_]](implicit F: Applicative[F]): Serializer[F, Double] =
    Serializer.delegate {
      (new org.apache.kafka.common.serialization.DoubleSerializer)
        .asInstanceOf[org.apache.kafka.common.serialization.Serializer[Double]]
    }

  implicit def float[F[_]](implicit F: Applicative[F]): Serializer[F, Float] =
    Serializer.delegate {
      (new org.apache.kafka.common.serialization.FloatSerializer)
        .asInstanceOf[org.apache.kafka.common.serialization.Serializer[Float]]
    }

  implicit def int[F[_]](implicit F: Applicative[F]): Serializer[F, Int] =
    Serializer.delegate {
      (new org.apache.kafka.common.serialization.IntegerSerializer)
        .asInstanceOf[org.apache.kafka.common.serialization.Serializer[Int]]
    }

  implicit def long[F[_]](implicit F: Applicative[F]): Serializer[F, Long] =
    Serializer.delegate {
      (new org.apache.kafka.common.serialization.LongSerializer)
        .asInstanceOf[org.apache.kafka.common.serialization.Serializer[Long]]
    }

  implicit def short[F[_]](implicit F: Applicative[F]): Serializer[F, Short] =
    Serializer.delegate {
      (new org.apache.kafka.common.serialization.ShortSerializer)
        .asInstanceOf[org.apache.kafka.common.serialization.Serializer[Short]]
    }

  implicit def string[F[_]](implicit F: Applicative[F]): Serializer[F, String] =
    Serializer.string(StandardCharsets.UTF_8)

  implicit def unit[F[_]](implicit F: Applicative[F]): Serializer[F, Unit] =
    Serializer.const(null)

  implicit def uuid[F[_]](implicit F: Applicative[F]): Serializer[F, UUID] =
    Serializer.string.contramap(_.toString)
}

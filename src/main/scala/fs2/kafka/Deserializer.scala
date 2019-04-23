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

import cats._
import cats.effect._
import cats.implicits._
import java.nio.charset.{Charset, StandardCharsets}
import java.util.UUID
import org.apache.kafka.common.utils.Bytes

/**
  * [[Deserializer]] is a functional Kafka deserializer which directly
  * extends the Kafka `Deserializer` interface, but doesn't make use
  * of `close` or `configure`. There is only a single function for
  * deserialization, which provides access to the record headers.
  */
sealed abstract class Deserializer[F[_], A] {

  /**
    * Deserializes the specified bytes into a value of type `A`. The
    * Kafka topic name, from which the serialized bytes came, and
    * record headers are available.
    */
  def deserialize(topic: String, headers: Headers, bytes: Array[Byte]): F[A]

  /**
    * Creates a new [[Deserializer]] which applies the specified
    * function `f` to the result of this [[Deserializer]].
    */
  final def map[B](f: A => B)(implicit F: Functor[F]): Deserializer[F, B] =
    Deserializer.instance { (topic, headers, bytes) =>
      deserialize(topic, headers, bytes).map(f)
    }

  /**
    * Creates a new [[Deserializer]] using the result of
    * this [[Deserializer]] and the specified function.
    */
  final def flatMap[B](f: A => Deserializer[F, B])(implicit F: FlatMap[F]): Deserializer[F, B] =
    Deserializer.instance { (topic, headers, bytes) =>
      deserialize(topic, headers, bytes).flatMap { a =>
        f(a).deserialize(topic, headers, bytes)
      }
    }

  /**
    * Creates a new [[Deserializer]] which deserializes both using
    * this [[Deserializer]] and that [[Deserializer]], and returns
    * both results in a tuple.
    */
  final def product[B](that: Deserializer[F, B])(implicit F: Apply[F]): Deserializer[F, (A, B)] =
    Deserializer.instance { (topic, headers, bytes) =>
      val a = deserialize(topic, headers, bytes)
      val b = that.deserialize(topic, headers, bytes)
      a product b
    }

  /**
    * Creates a new [[Deserializer]] which catches any non-fatal
    * exceptions during deserialization with this [[Deserializer]].
    */
  final def attempt(implicit F: ApplicativeError[F, Throwable]): Deserializer[F, A] =
    Deserializer.instance { (topic, headers, bytes) =>
      Either
        .catchNonFatal(deserialize(topic, headers, bytes))
        .fold(F.raiseError, identity)
    }

  /**
    * Creates a new [[Deserializer]] which returns `None` when the
    * bytes are `null`, and otherwise returns the result of this
    * [[Deserializer]] wrapped in `Some`.
    */
  final def option(implicit F: Applicative[F]): Deserializer[F, Option[A]] =
    Deserializer.instance { (topic, headers, bytes) =>
      if (bytes != null)
        deserialize(topic, headers, bytes).map(Some.apply)
      else
        F.pure(None)
    }

  /**
    * Creates a new [[Deserializer]] which defers deserialization.
    */
  final def defer(implicit F: Defer[F]): Deserializer[F, A] =
    Deserializer.instance { (topic, headers, bytes) =>
      F.defer(deserialize(topic, headers, bytes))
    }

  /**
    * Creates a new [[Deserializer]] which suspends deserialization,
    * capturing any impure behaviours of this [[Deserializer]].
    */
  final def suspend(implicit F: Sync[F]): Deserializer[F, A] =
    Deserializer.instance { (topic, headers, bytes) =>
      F.suspend(deserialize(topic, headers, bytes))
    }
}

object Deserializer {
  def apply[F[_], A](implicit deserializer: Deserializer[F, A]): Deserializer[F, A] = deserializer

  /**
    * Creates a new [[Deserializer]] which deserializes
    * all bytes to the specified value of type `A`.
    */
  def const[F[_], A](a: A)(implicit F: Applicative[F]): Deserializer[F, A] =
    Deserializer.lift(_ => F.pure(a))

  /**
    * Creates a new [[Deserializer]] which delegates deserialization
    * to the specified Kafka `Deserializer`. Note that the `close`
    * and `configure` functions won't be called for the delegate.<br>
    * <br>
    * It is assumed the delegate `deserialize` function is pure.
    * If it's not pure, then use `suspend` after `delegate`,
    * so the impure behaviours can be captured properly.
    */
  def delegate[F[_], A](deserializer: KafkaDeserializer[A])(
    implicit F: Applicative[F]
  ): Deserializer[F, A] =
    Deserializer.instance { (topic, headers, bytes) =>
      F.pure(deserializer.deserialize(topic, headers.asJava, bytes))
    }

  /**
    * Creates a new [[Deserializer]] which always fails
    * deserialization with the specified exception `e`.
    */
  def fail[F[_], A](e: Throwable)(
    implicit F: ApplicativeError[F, Throwable]
  ): Deserializer[F, A] =
    Deserializer.lift(_ => F.raiseError(e))

  /**
    * Creates a new [[Deserializer]] which can use different
    * [[Deserializer]]s depending on the record headers.
    */
  def headers[F[_], A](f: Headers => Deserializer[F, A]): Deserializer[F, A] =
    Deserializer.instance { (topic, headers, bytes) =>
      f(headers).deserialize(topic, headers, bytes)
    }

  /**
    * Creates a new [[Deserializer]] from the specified function.
    * Use [[lift]] instead if the deserializer doesn't need
    * access to the Kafka topic name or record headers.
    */
  def instance[F[_], A](f: (String, Headers, Array[Byte]) => F[A]): Deserializer[F, A] =
    new Deserializer[F, A] {
      override def deserialize(topic: String, headers: Headers, bytes: Array[Byte]): F[A] =
        f(topic, headers, bytes)

      override def toString: String =
        "Deserializer$" + System.identityHashCode(this)
    }

  /**
    * Creates a new [[Deserializer]] from the specified function,
    * ignoring from which Kafka topic the bytes came and any
    * record headers. Use [[instance]] instead if the
    * deserializer needs access to the Kafka topic
    * name or the record headers.
    */
  def lift[F[_], A](f: Array[Byte] => F[A]): Deserializer[F, A] =
    Deserializer.instance((_, _, bytes) => f(bytes))

  /**
    * Creates a new [[Deserializer]] which can use different
    * [[Deserializer]]s depending on the Kafka topic name
    * from which the serialized bytes came.
    */
  def topic[F[_], A](f: String => Deserializer[F, A]): Deserializer[F, A] =
    Deserializer.instance { (topic, headers, bytes) =>
      f(topic).deserialize(topic, headers, bytes)
    }

  /**
    * Creates a new [[Deserializer]] which deserializes `String`
    * values using the specified `Charset`. Note that the
    * default `String` deserializer uses `UTF-8`.
    */
  def string[F[_]](charset: Charset)(implicit F: Applicative[F]): Deserializer[F, String] =
    Deserializer.lift(bytes => F.pure(new String(bytes, charset)))

  /**
    * Creates a new [[Deserializer]] which deserializes `String`
    * values using the specified `Charset` as `UUID`s. Note that
    * the default `UUID` deserializer uses `UTF-8`.
    */
  def uuid[F[_]](
    charset: Charset
  )(implicit F: ApplicativeError[F, Throwable]): Deserializer[F, UUID] =
    Deserializer.string[F](charset).map(UUID.fromString).attempt

  /**
    * The identity [[Deserializer]], which does not perform any kind
    * of deserialization, simply using the input bytes as the output.
    */
  implicit def identity[F[_]](implicit F: Applicative[F]): Deserializer[F, Array[Byte]] =
    Deserializer.lift(bytes => F.pure(bytes))

  /**
    * The option [[Deserializer]] returns `None` when the bytes are
    * `null`, and otherwise deserializes using the deserializer for
    * the type `A`, wrapping the result in `Some`.
    */
  implicit def option[F[_], A](
    implicit F: Applicative[F],
    deserializer: Deserializer[F, A]
  ): Deserializer[F, Option[A]] =
    deserializer.option

  implicit def monad[F[_]](implicit F: Monad[F]): Monad[Deserializer[F, ?]] =
    new Monad[Deserializer[F, ?]] {
      override def pure[A](a: A): Deserializer[F, A] =
        Deserializer.const(a)

      override def map[A, B](
        deserializer: Deserializer[F, A]
      )(f: A => B): Deserializer[F, B] =
        deserializer.map(f)

      override def flatMap[A, B](
        deserializer: Deserializer[F, A]
      )(f: A => Deserializer[F, B]): Deserializer[F, B] =
        deserializer.flatMap(f)

      override def product[A, B](
        first: Deserializer[F, A],
        second: Deserializer[F, B]
      ): Deserializer[F, (A, B)] =
        first.product(second)

      override def tailRecM[A, B](a: A)(f: A => Deserializer[F, Either[A, B]]): Deserializer[F, B] =
        Deserializer.instance { (topic, headers, bytes) =>
          F.tailRecM(a)(f(_).deserialize(topic, headers, bytes))
        }
    }

  implicit def bytes[F[_]](implicit F: Applicative[F]): Deserializer[F, Bytes] =
    Deserializer.lift(bytes => F.pure(new Bytes(bytes)))

  implicit def double[F[_]](implicit F: ApplicativeError[F, Throwable]): Deserializer[F, Double] =
    Deserializer
      .delegate[F, Double] {
        (new org.apache.kafka.common.serialization.DoubleDeserializer)
          .asInstanceOf[org.apache.kafka.common.serialization.Deserializer[Double]]
      }
      .attempt

  implicit def float[F[_]](implicit F: ApplicativeError[F, Throwable]): Deserializer[F, Float] =
    Deserializer
      .delegate[F, Float] {
        (new org.apache.kafka.common.serialization.FloatDeserializer)
          .asInstanceOf[org.apache.kafka.common.serialization.Deserializer[Float]]
      }
      .attempt

  implicit def int[F[_]](implicit F: ApplicativeError[F, Throwable]): Deserializer[F, Int] =
    Deserializer
      .delegate[F, Int] {
        (new org.apache.kafka.common.serialization.IntegerDeserializer)
          .asInstanceOf[org.apache.kafka.common.serialization.Deserializer[Int]]
      }
      .attempt

  implicit def long[F[_]](implicit F: ApplicativeError[F, Throwable]): Deserializer[F, Long] =
    Deserializer
      .delegate[F, Long] {
        (new org.apache.kafka.common.serialization.LongDeserializer)
          .asInstanceOf[org.apache.kafka.common.serialization.Deserializer[Long]]
      }
      .attempt

  implicit def short[F[_]](implicit F: ApplicativeError[F, Throwable]): Deserializer[F, Short] =
    Deserializer
      .delegate[F, Short] {
        (new org.apache.kafka.common.serialization.ShortDeserializer)
          .asInstanceOf[org.apache.kafka.common.serialization.Deserializer[Short]]
      }
      .attempt

  implicit def string[F[_]](implicit F: Applicative[F]): Deserializer[F, String] =
    Deserializer.string(StandardCharsets.UTF_8)

  implicit def unit[F[_]](implicit F: Applicative[F]): Deserializer[F, Unit] =
    Deserializer.const(())

  implicit def uuid[F[_]](implicit F: ApplicativeError[F, Throwable]): Deserializer[F, UUID] =
    Deserializer.string[F].map(UUID.fromString).attempt
}

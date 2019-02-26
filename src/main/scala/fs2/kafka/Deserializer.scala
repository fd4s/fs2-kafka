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

import cats.{Eval, Monad}
import cats.syntax.either._
import fs2.kafka.internal.syntax._
import java.nio.charset.{Charset, StandardCharsets}
import java.util.UUID
import org.apache.kafka.common.utils.Bytes
import scala.annotation.tailrec

/**
  * [[Deserializer]] is a functional Kafka deserializer which directly
  * extends the Kafka `Deserializer` interface, but doesn't make use
  * of `close` or `configure`. There is only a single function for
  * deserialization, which provides access to the record headers.
  */
sealed abstract class Deserializer[A] extends KafkaDeserializer[A] {

  /**
    * Deserializes the specified bytes into a value of type `A`. The
    * Kafka topic name, from which the serialized bytes came, and
    * record headers are available.
    */
  def deserialize(topic: String, headers: Headers, bytes: Array[Byte]): A

  /**
    * Creates a new [[Deserializer]] which applies the specified
    * function `f` to the result of this [[Deserializer]].
    */
  final def map[B](f: A => B): Deserializer[B] =
    Deserializer.instance { (topic, headers, bytes) =>
      f(deserialize(topic, headers, bytes))
    }

  /**
    * Creates a new [[Deserializer]] using the result of
    * this [[Deserializer]] and the specified function.
    */
  final def flatMap[B](f: A => Deserializer[B]): Deserializer[B] =
    Deserializer.instance { (topic, headers, bytes) =>
      f(deserialize(topic, headers, bytes))
        .deserialize(topic, headers, bytes)
    }

  /**
    * Creates a new [[Deserializer]] which deserializes both using
    * this [[Deserializer]] and that [[Deserializer]], and returns
    * both results in a tuple.
    */
  final def product[B](that: Deserializer[B]): Deserializer[(A, B)] =
    Deserializer.instance { (topic, headers, bytes) =>
      val a = deserialize(topic, headers, bytes)
      val b = that.deserialize(topic, headers, bytes)
      (a, b)
    }

  /**
    * Creates a new [[Deserializer]] which does deserialization
    * lazily by wrapping this [[Deserializer]] in `Eval.always`.
    */
  final def delay: Deserializer[Eval[A]] =
    Deserializer.instance { (topic, headers, bytes) =>
      Eval.always(deserialize(topic, headers, bytes))
    }

  /**
    * Creates a new [[Deserializer]] which catches any non-fatal
    * exceptions during deserialization with this [[Deserializer]].
    */
  final def attempt: Deserializer.Attempt[A] =
    Deserializer.instance { (topic, headers, bytes) =>
      Either.catchNonFatal(deserialize(topic, headers, bytes))
    }

  /** For interoperability with Kafka deserialization. */
  final override def deserialize(topic: String, bytes: Array[Byte]): A =
    deserialize(topic, Headers.empty, bytes)

  /** For interoperability with Kafka deserialization. */
  final override def deserialize(topic: String, headers: KafkaHeaders, bytes: Array[Byte]): A =
    deserialize(topic, headers.asScala, bytes)

  /** Always only returns `Unit`. For interoperability with Kafka deserialization. */
  final override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = ()

  /** Always only returns `Unit`. For interoperability with Kafka deserialization. */
  final override def close(): Unit = ()
}

object Deserializer {

  /** Alias for `Deserializer[Either[Throwable, A]]`. */
  type Attempt[A] = Deserializer[Either[Throwable, A]]

  def apply[A](
    implicit deserializer: Deserializer[A]
  ): Deserializer[A] = deserializer

  def attempt[A](
    implicit deserializer: Deserializer.Attempt[A]
  ): Deserializer.Attempt[A] = deserializer

  /**
    * Creates a new [[Deserializer]] which deserializes
    * all bytes to the specified value of type `A`.
    */
  def const[A](a: A): Deserializer[A] =
    Deserializer.lift(_ => a)

  /**
    * Creates a new [[Deserializer]] which delegates deserialization
    * to the specified Kafka `Deserializer`. Note that the `close`
    * and `configure` functions won't be called for the delegate.
    */
  def delegate[A](deserializer: KafkaDeserializer[A]): Deserializer[A] =
    Deserializer.instance { (topic, headers, bytes) =>
      deserializer.deserialize(topic, headers.asJava, bytes)
    }

  /**
    * Creates a new [[Deserializer]] which can use different
    * [[Deserializer]]s depending on the record headers.
    */
  def headers[A](f: Headers => Deserializer[A]): Deserializer[A] =
    Deserializer.instance { (topic, headers, bytes) =>
      f(headers).deserialize(topic, headers, bytes)
    }

  /**
    * Creates a new [[Deserializer]] from the specified function.
    * Use [[lift]] instead if the deserializer doesn't need
    * access to the Kafka topic name or record headers.
    */
  def instance[A](f: (String, Headers, Array[Byte]) => A): Deserializer[A] =
    new Deserializer[A] {
      override def deserialize(topic: String, headers: Headers, bytes: Array[Byte]): A =
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
  def lift[A](f: Array[Byte] => A): Deserializer[A] =
    Deserializer.instance((_, _, bytes) => f(bytes))

  /**
    * Creates a new [[Deserializer]] which can use different
    * [[Deserializer]]s depending on the Kafka topic name
    * from which the serialized bytes came.
    */
  def topic[A](f: String => Deserializer[A]): Deserializer[A] =
    Deserializer.instance { (topic, headers, bytes) =>
      f(topic).deserialize(topic, headers, bytes)
    }

  /**
    * Creates a new [[Deserializer]] which deserializes `String`
    * values using the specified `Charset`. Note that the
    * default `String` deserializer uses `UTF-8`.
    */
  def string(charset: Charset): Deserializer[String] =
    Deserializer.lift(bytes => new String(bytes, charset))

  /**
    * Creates a new [[Deserializer]] which deserializes `String`
    * values using the specified `Charset` as `UUID`s. Note that
    * the default `UUID` deserializer uses `UTF-8`.
    */
  def uuid(charset: Charset): Deserializer.Attempt[UUID] =
    Deserializer.string(charset).map(UUID.fromString).attempt

  /**
    * The identity [[Deserializer]], which does not perform any kind
    * of deserialization, simply using the input bytes as the output.
    */
  implicit val identity: Deserializer[Array[Byte]] =
    Deserializer.lift(bytes => bytes)

  implicit val monad: Monad[Deserializer] =
    new Monad[Deserializer] {
      override def pure[A](a: A): Deserializer[A] =
        Deserializer.const(a)

      override def map[A, B](
        deserializer: Deserializer[A]
      )(f: A => B): Deserializer[B] =
        deserializer.map(f)

      override def flatMap[A, B](
        deserializer: Deserializer[A]
      )(f: A => Deserializer[B]): Deserializer[B] =
        deserializer.flatMap(f)

      override def product[A, B](
        first: Deserializer[A],
        second: Deserializer[B]
      ): Deserializer[(A, B)] =
        first.product(second)

      override def tailRecM[A, B](a: A)(f: A => Deserializer[Either[A, B]]): Deserializer[B] =
        Deserializer.instance { (topic, headers, bytes) =>
          @tailrec def go(deserializer: Deserializer[Either[A, B]]): B =
            deserializer.deserialize(topic, headers, bytes) match {
              case Right(b) => b
              case Left(a)  => go(f(a))
            }

          go(f(a))
        }
    }

  implicit val bytes: Deserializer[Bytes] =
    Deserializer.identity.map(bytes => new Bytes(bytes))

  implicit val double: Deserializer.Attempt[Double] =
    Deserializer.delegate {
      (new org.apache.kafka.common.serialization.DoubleDeserializer)
        .asInstanceOf[org.apache.kafka.common.serialization.Deserializer[Double]]
    }.attempt

  implicit val float: Deserializer.Attempt[Float] =
    Deserializer.delegate {
      (new org.apache.kafka.common.serialization.FloatDeserializer)
        .asInstanceOf[org.apache.kafka.common.serialization.Deserializer[Float]]
    }.attempt

  implicit val int: Deserializer.Attempt[Int] =
    Deserializer.delegate {
      (new org.apache.kafka.common.serialization.IntegerDeserializer)
        .asInstanceOf[org.apache.kafka.common.serialization.Deserializer[Int]]
    }.attempt

  implicit val long: Deserializer.Attempt[Long] =
    Deserializer.delegate {
      (new org.apache.kafka.common.serialization.LongDeserializer)
        .asInstanceOf[org.apache.kafka.common.serialization.Deserializer[Long]]
    }.attempt

  implicit val short: Deserializer.Attempt[Short] =
    Deserializer.delegate {
      (new org.apache.kafka.common.serialization.ShortDeserializer)
        .asInstanceOf[org.apache.kafka.common.serialization.Deserializer[Short]]
    }.attempt

  implicit val string: Deserializer[String] =
    Deserializer.string(StandardCharsets.UTF_8)

  implicit val uuid: Deserializer.Attempt[UUID] =
    Deserializer.string.map(UUID.fromString).attempt
}

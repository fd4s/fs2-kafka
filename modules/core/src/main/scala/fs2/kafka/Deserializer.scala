/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.MonadError
import cats.effect.Sync
import java.nio.charset.{Charset, StandardCharsets}
import java.util.UUID

/**
  * Functional composable Kafka key deserializer with
  * support for effect types.
  */
sealed trait KeyDeserializer[F[_], A] extends GenDeserializer[KeyDeserializer, F, A]

/**
  * Functional composable Kafka value deserializer with
  * support for effect types.
  */
sealed trait ValueDeserializer[F[_], A] extends GenDeserializer[ValueDeserializer, F, A]

/**
  * Functional composable Kafka deserializer that can deserialize
  * both record keys and record values.
  */
sealed abstract class Deserializer[F[_], A]
    extends GenDeserializer[Deserializer, F, A]
    with KeyDeserializer[F, A]
    with ValueDeserializer[F, A] {
  def forKey: KeyDeserializer[F, A] = this
  def forValue: ValueDeserializer[F, A] = this
}

object Deserializer {
  def apply[F[_], A](implicit deserializer: Deserializer[F, A]): Deserializer[F, A] = deserializer

  /** Alias for [[Deserializer#identity]]. */
  def apply[F[_]](implicit F: Sync[F]): Deserializer[F, Array[Byte]] = identity

  /**
    * Creates a new [[Deserializer]] which deserializes
    * all bytes to the specified value of type `A`.
    */
  def const[F[_], A](a: A)(implicit F: Sync[F]): Deserializer[F, A] =
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
  def delegate[F[_], A](
    deserializer: KafkaDeserializer[A]
  )(implicit F: Sync[F]): Deserializer[F, A] =
    Deserializer.instance { (topic, headers, bytes) =>
      F.pure(deserializer.deserialize(topic, headers.asJava, bytes))
    }

  /**
    * Creates a new [[Deserializer]] which always fails
    * deserialization with the specified exception `e`.
    */
  def fail[F[_], A](e: Throwable)(
    implicit F: Sync[F]
  ): Deserializer[F, A] =
    Deserializer.lift(_ => F.raiseError(e))

  /**
    * Creates a new [[Deserializer]] which always fails
    * deserialization with a [[DeserializationException]]
    * using the specified message.
    */
  def failWith[F[_], A](message: String)(implicit F: Sync[F]): Deserializer[F, A] =
    Deserializer.fail(DeserializationException(message))

  /**
    * Creates a new [[Deserializer]] which can use different
    * [[Deserializer]]s depending on the record headers.
    */
  def headers[F[_], A](f: Headers => Deserializer[F, A])(implicit F: Sync[F]): Deserializer[F, A] =
    Deserializer.instance { (topic, headers, bytes) =>
      f(headers).deserialize(topic, headers, bytes)
    }

  /**
    * Creates a new [[Deserializer]] from the specified function.
    * Use [[lift]] instead if the deserializer doesn't need
    * access to the Kafka topic name or record headers.
    */
  def instance[F[_], A](
    f: (String, Headers, Array[Byte]) => F[A]
  )(implicit F: Sync[F]): Deserializer[F, A] =
    new Deserializer[F, A] {
      import cats.syntax.all._

      override def deserialize(topic: String, headers: Headers, bytes: Array[Byte]): F[A] =
        f(topic, headers, bytes)

      override def map[B](f: A => B): Deserializer[F, B] =
        Deserializer.instance { (topic, headers, bytes) =>
          deserialize(topic, headers, bytes).map(f)
        }

      override def flatMap[That[f[_], x] >: Deserializer[f, x] <: GenDeserializer[That, f, x], B](
        f: A => That[F, B]
      ): That[F, B] =
        instance { (topic, headers, bytes) =>
          F.flatMap(deserialize(topic, headers, bytes)) { a =>
            f(a).deserialize(topic, headers, bytes)
          }
        }

      override def product[That[f[_], x] >: Deserializer[f, x] <: GenDeserializer[That, f, x], B](
        that: That[F, B]
      ): That[F, (A, B)] =
        Deserializer.instance { (topic, headers, bytes) =>
          val a = this.deserialize(topic, headers, bytes)
          val b = that.deserialize(topic, headers, bytes)
          F.product(a, b)
        }

      override def attempt: Deserializer[F, Either[Throwable, A]] =
        Deserializer.instance { (topic, headers, bytes) =>
          deserialize(topic, headers, bytes).attempt
        }

      override def option: Deserializer[F, Option[A]] =
        Deserializer.instance { (topic, headers, bytes) =>
          if (bytes != null)
            deserialize(topic, headers, bytes).map(Some.apply)
          else
            F.pure(None)
        }

      override def suspend: Deserializer[F, A] =
        Deserializer.instance { (topic, headers, bytes) =>
          F.defer(deserialize(topic, headers, bytes))
        }

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
  def lift[F[_], A](f: Array[Byte] => F[A])(implicit F: Sync[F]): Deserializer[F, A] =
    Deserializer.instance((_, _, bytes) => f(bytes))

  private[this] def unexpectedTopic[F[_], A](implicit F: Sync[F]): String => Deserializer[F, A] =
    topic => Deserializer.fail(UnexpectedTopicException(topic))

  /**
    * Creates a new [[Deserializer]] which can use different
    * [[Deserializer]]s depending on the Kafka topic name
    * from which the serialized bytes came.
    */
  def topic[This[f[_], a] >: Deserializer[f, a] <: GenDeserializer[This, f, a], F[_], A](
    f: PartialFunction[String, This[F, A]]
  )(implicit F: Sync[F]): This[F, A] =
    Deserializer.instance { (topic, headers, bytes) =>
      f.applyOrElse(topic, unexpectedTopic)
        .deserialize(topic, headers, bytes)
    }

  /**
    * Creates a new [[Deserializer]] which deserializes `String`
    * values using the specified `Charset`. Note that the
    * default `String` deserializer uses `UTF-8`.
    */
  def string[F[_]](charset: Charset)(implicit F: Sync[F]): Deserializer[F, String] =
    Deserializer.lift(bytes => F.pure(new String(bytes, charset)))

  /**
    * Creates a new [[Deserializer]] which deserializes `String`
    * values using the specified `Charset` as `UUID`s. Note that
    * the default `UUID` deserializer uses `UTF-8`.
    */
  def uuid[F[_]](charset: Charset)(implicit F: Sync[F]): Deserializer[F, UUID] =
    Deserializer.string[F](charset).map(UUID.fromString).suspend

  /**
    * The identity [[Deserializer]], which does not perform any kind
    * of deserialization, simply using the input bytes as the output.
    */
  def identity[F[_]](implicit F: Sync[F]): Deserializer[F, Array[Byte]] =
    Deserializer.lift(bytes => F.pure(bytes))

  /**
    * The option [[Deserializer]] returns `None` when the bytes are
    * `null`, and otherwise deserializes using the deserializer for
    * the type `A`, wrapping the result in `Some`.
    */
  def option[This[f[_], a] <: GenDeserializer[This, f, a], F[_], A](
    implicit deserializer: This[F, A]
  ): This[F, Option[A]] =
    deserializer.option

  def double[F[_]](implicit F: Sync[F]): Deserializer[F, Double] =
    Deserializer
      .delegate[F, Double] {
        (new org.apache.kafka.common.serialization.DoubleDeserializer)
          .asInstanceOf[org.apache.kafka.common.serialization.Deserializer[Double]]
      }
      .suspend

  def float[F[_]](implicit F: Sync[F]): Deserializer[F, Float] =
    Deserializer
      .delegate[F, Float] {
        (new org.apache.kafka.common.serialization.FloatDeserializer)
          .asInstanceOf[org.apache.kafka.common.serialization.Deserializer[Float]]
      }
      .suspend

  def int[F[_]](implicit F: Sync[F]): Deserializer[F, Int] =
    Deserializer
      .delegate[F, Int] {
        (new org.apache.kafka.common.serialization.IntegerDeserializer)
          .asInstanceOf[org.apache.kafka.common.serialization.Deserializer[Int]]
      }
      .suspend

  def long[F[_]](implicit F: Sync[F]): Deserializer[F, Long] =
    Deserializer
      .delegate[F, Long] {
        (new org.apache.kafka.common.serialization.LongDeserializer)
          .asInstanceOf[org.apache.kafka.common.serialization.Deserializer[Long]]
      }
      .suspend

  def short[F[_]](implicit F: Sync[F]): Deserializer[F, Short] =
    Deserializer
      .delegate[F, Short] {
        (new org.apache.kafka.common.serialization.ShortDeserializer)
          .asInstanceOf[org.apache.kafka.common.serialization.Deserializer[Short]]
      }
      .suspend

  def string[F[_]](implicit F: Sync[F]): Deserializer[F, String] =
    Deserializer.string(StandardCharsets.UTF_8)

  def unit[F[_]](implicit F: Sync[F]): Deserializer[F, Unit] =
    Deserializer.const(())

  def uuid[F[_]](implicit F: Sync[F]): Deserializer[F, UUID] =
    Deserializer.string[F].map(UUID.fromString).suspend
}

// format: off
sealed abstract class GenDeserializer[+This[f[_], x] <: GenDeserializer[This, f, x], F[_], A] {
  // format: on
  self: This[F, A] =>

  /**
    * Attempts to deserialize the specified bytes into a value of
    * type `A`. The Kafka topic name, from which the serialized
    * bytes came, and record headers are available.
    */
  def deserialize(topic: String, headers: Headers, bytes: Array[Byte]): F[A]

  /**
    * Creates a new [[Deserializer]] which applies the specified
    * function to the result of this [[Deserializer]].
    */
  def map[B](f: A => B): This[F, B]

  /**
    * Creates a new [[Deserializer]] by first deserializing
    * with this [[Deserializer]] and then using the result
    * as input to the specified function.
    */
  def flatMap[That[f[_], x] >: This[f, x] <: GenDeserializer[That, f, x], B](
    f: A => That[F, B]
  ): That[F, B]

  /**
    * Creates a new [[Deserializer]] which deserializes both using
    * this [[Deserializer]] and that [[Deserializer]], and returns
    * both results in a tuple.
    */
  def product[That[f[_], x] >: This[f, x] <: GenDeserializer[That, f, x], B](
    that: That[F, B]
  ): That[F, (A, B)]

  /**
    * Creates a new [[Deserializer]] which handles errors by
    * turning them into `Either` values.
    */
  def attempt: This[F, Either[Throwable, A]]

  /**
    * Creates a new [[Deserializer]] which returns `None` when the
    * bytes are `null`, and otherwise returns the result of this
    * [[Deserializer]] wrapped in `Some`.
    */
  def option: This[F, Option[A]]

  /**
    * Creates a new [[Deserializer]] which suspends deserialization,
    * capturing any impure behaviours of this [[Deserializer]].
    */
  def suspend: This[F, A]
}

object GenDeserializer {
  implicit def monadError[F[_], This[g[_], x] >: Deserializer[g, x] <: GenDeserializer[This, g, x]](
    implicit F: Sync[F]
  ): MonadError[This[F, *], Throwable] =
    new MonadError[This[F, *], Throwable] {
      override def pure[A](a: A): This[F, A] =
        Deserializer.const(a)

      override def map[A, B](
        deserializer: This[F, A]
      )(f: A => B): This[F, B] =
        deserializer.map(f)

      override def flatMap[A, B](
        deserializer: This[F, A]
      )(f: A => This[F, B]): This[F, B] =
        deserializer.flatMap(f)

      override def product[A, B](
        first: This[F, A],
        second: This[F, B]
      ): This[F, (A, B)] =
        first.product(second)

      override def tailRecM[A, B](a: A)(f: A => This[F, Either[A, B]]): This[F, B] =
        Deserializer.instance { (topic, headers, bytes) =>
          F.tailRecM(a)(f(_).deserialize(topic, headers, bytes))
        }

      override def handleErrorWith[A](fa: This[F, A])(
        f: Throwable => This[F, A]
      ): This[F, A] =
        Deserializer.instance { (topic, headers, bytes) =>
          F.handleErrorWith(fa.deserialize(topic, headers, bytes)) { throwable =>
            f(throwable).deserialize(topic, headers, bytes)
          }
        }

      override def raiseError[A](e: Throwable): This[F, A] =
        Deserializer.fail[F, A](e)
    }

  implicit def identity[F[_]](implicit F: Sync[F]): Deserializer[F, Array[Byte]] =
    Deserializer.identity

  implicit def option[This[f[_], a] >: Deserializer[f, a] <: GenDeserializer[This, f, a], F[_], A](
    implicit deserializer: This[F, A]
  ): This[F, Option[A]] =
    deserializer.option

  implicit def double[F[_]](implicit F: Sync[F]): Deserializer[F, Double] =
    Deserializer.double

  implicit def float[F[_]](implicit F: Sync[F]): Deserializer[F, Float] =
    Deserializer.float

  implicit def int[F[_]](implicit F: Sync[F]): Deserializer[F, Int] =
    Deserializer.int

  implicit def long[F[_]](implicit F: Sync[F]): Deserializer[F, Long] =
    Deserializer.long

  implicit def short[F[_]](implicit F: Sync[F]): Deserializer[F, Short] =
    Deserializer.short

  implicit def string[F[_]](implicit F: Sync[F]): Deserializer[F, String] =
    Deserializer.string

  implicit def unit[F[_]](implicit F: Sync[F]): Deserializer[F, Unit] =
    Deserializer.unit

  implicit def uuid[F[_]](implicit F: Sync[F]): Deserializer[F, UUID] =
    Deserializer.uuid
}

/*
 * Copyright 2018-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.effect.unsafe.implicits.global
import cats.effect.IO
import fs2.kafka._
import fs2.kafka.BaseSpec
import fs2.kafka.internal.syntax._
import org.apache.kafka.common.KafkaFuture

import java.time.temporal.ChronoUnit.MICROS
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.internals.KafkaFutureImpl

import java.util.concurrent.CancellationException
import scala.concurrent.duration._

final class SyntaxSpec extends BaseSpec {
  describe("FiniteDuration#asJava") {
    it("should convert days") { assert(1.day.asJava == java.time.Duration.ofDays(1)) }
    it("should convert hours") { assert(1.hour.asJava == java.time.Duration.ofHours(1)) }
    it("should convert minutes") { assert(1.minute.asJava == java.time.Duration.ofMinutes(1)) }
    it("should convert seconds") { assert(1.second.asJava == java.time.Duration.ofSeconds(1)) }
    it("should convert millis") { assert(1.milli.asJava == java.time.Duration.ofMillis(1)) }
    it("should convert micros") { assert(1.micro.asJava == java.time.Duration.of(1, MICROS)) }
    it("should convert nanos") { assert(1.nanos.asJava == java.time.Duration.ofNanos(1)) }
  }

  describe("Map#filterKeysStrictValuesList") {
    it("should be the same as toList.collect") {
      forAll { (m: Map[Int, Int], p: Int => Boolean) =>
        assert {
          m.filterKeysStrictValuesList(p) == m.toList.collect { case (k, v) if (p(k)) => v }
        }
      }
    }
  }

  describe("KafkaHeaders#asScala") {
    it("should convert empty") {
      val kafkaHeaders: KafkaHeaders =
        new RecordHeaders()

      assert(kafkaHeaders.asScala == Headers.empty)
    }

    it("should convert non-empty") {
      val kafkaHeaders: KafkaHeaders = {
        val recordHeaders = new RecordHeaders()
        recordHeaders.add("key", Array())
        recordHeaders
      }

      assert {
        val headers = kafkaHeaders.asScala
        headers.toChain.size == 1 &&
        headers("key").map(_.value.size) == Some(0)
      }
    }
  }

  describe("KafkaFuture.cancelable") {

    it("should cancel future when fiber is cancelled") {

      @volatile var isFutureCancelled = false

      val test =
        for {
          gate <- IO.deferred[Unit]
          futureIO: IO[KafkaFuture[Unit]] = gate.complete(()) >> IO {
            // We need to return the original future after calling `whenComplete`, because the future returned by
            // `whenComplete` doesn't propagate cancellation back to the original future.
            val future = new KafkaFutureImpl[Unit]
            future.whenComplete {
              case (_, _: CancellationException) => isFutureCancelled = true
              case _                             => ()
            }
            future
          }
          fiber <- futureIO.cancelable.start
          _ <- gate.get // wait for future to be created before canceling it
          _ <- IO(assert(!isFutureCancelled))
          _ <- fiber.cancel
          _ <- IO(assert(isFutureCancelled))
        } yield ()
      test.unsafeRunSync()
    }

  }
}

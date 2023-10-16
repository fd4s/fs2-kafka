/*
 * Copyright 2018-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.effect.unsafe.implicits.global
import cats.effect.IO
import cats.effect.std.CountDownLatch
import fs2.kafka._
import fs2.kafka.BaseSpec
import fs2.kafka.internal.syntax._
import org.apache.kafka.common.KafkaFuture

import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.internals.KafkaFutureImpl

import java.util.concurrent.CancellationException
import java.util.concurrent.atomic.AtomicBoolean

final class SyntaxSpec extends BaseSpec {
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
      val test =
        for {
          latch <- CountDownLatch[IO](1)
          isFutureStarted <- IO(new AtomicBoolean)
          isFutureCancelled <- IO(new AtomicBoolean)
          futureIO: IO[KafkaFuture[Unit]] = IO {
            isFutureStarted.set(true)
            // We need to return the original future after calling `whenComplete`, because the future returned by
            // `whenComplete` doesn't propagate cancellation back to the original future.
            val future = new KafkaFutureImpl[Unit]
            future.whenComplete {
              case (_, _: CancellationException) =>
                latch.release.as(isFutureCancelled.set(true)).unsafeRunSync()
              case _ => ()
            }
            future
          }
          fiber <- futureIO.cancelable_.start
          _ <- IO.cede.whileM_(IO(!isFutureStarted.get))
          _ <- IO(assert(!isFutureCancelled.get))
          _ <- fiber.cancel
          _ <- latch.await
          _ <- IO(assert(isFutureCancelled.get))
        } yield ()
      test.unsafeRunSync()
    }
  }
}

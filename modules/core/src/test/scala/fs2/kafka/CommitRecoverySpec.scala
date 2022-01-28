package fs2.kafka

import cats.data.Chain
import cats.effect.{Clock, IO}
import cats.syntax.functor._
import org.apache.kafka.clients.consumer.{OffsetAndMetadata, RetriableCommitFailedException}
import org.apache.kafka.common.TopicPartition

import scala.concurrent.duration.{FiniteDuration, TimeUnit}
import cats.effect.{ Ref, Temporal }

final class CommitRecoverySpec extends BaseAsyncSpec {
  describe("CommitRecovery#Default") {
    it("should retry with jittered exponential backoff and fixed rate") {
      val (result, sleeps) =
        Ref
          .of[IO, Chain[FiniteDuration]](Chain.empty)
          .flatMap { ref =>
            implicit val timer: Temporal[IO] = storeSleepsTimer(ref)
            val commit: IO[Unit] = IO.raiseError(new RetriableCommitFailedException("retriable"))
            val offsets = Map(new TopicPartition("topic", 0) -> new OffsetAndMetadata(1))
            val recovery = CommitRecovery.Default.recoverCommitWith(offsets, commit)
            val attempted = commit.handleErrorWith(recovery).attempt
            attempted.flatMap(ref.get.tupleLeft)
          }
          .unsafeRunSync()

      assert {
        result.left.toOption.map(_.toString).contains {
          "fs2.kafka.CommitRecoveryException: offset commit is still failing after 15 attempts for offsets: topic-0 -> 1; last exception was: org.apache.kafka.clients.consumer.RetriableCommitFailedException: retriable"
        }
      }

      assert { sleeps.size == 15L }

      assert {
        sleeps.toList.take(10).zipWithIndex.forall {
          case (sleep, attempt) =>
            val max = 10 * Math.pow(2, attempt.toDouble + 1)
            0 <= sleep.toMillis && sleep.toMillis < max
        }
      }

      assert { sleeps.toList.drop(10).forall(_.toMillis == 10000L) }
    }

    it("should not recover non-retriable exceptions") {
      val commit: IO[Unit] = IO.raiseError(new RuntimeException("commit"))
      val retry: IO[Unit] = IO.raiseError(new RuntimeException("retry"))
      val recovery = CommitRecovery.Default.recoverCommitWith(Map(), retry)
      val result = commit.handleErrorWith(recovery).attempt.unsafeRunSync()
      assert { result.left.toOption.map(_.getMessage).contains("commit") }
    }

    it("should have Default as String representation") {
      assert(CommitRecovery.Default.toString == "Default")
    }
  }

  describe("CommitRecovery#None") {
    it("should not recover any exceptions") {
      val commit: IO[Unit] = IO.raiseError(new RuntimeException("commit"))
      val retry: IO[Unit] = IO.raiseError(new RuntimeException("retry"))
      val recovery = CommitRecovery.None.recoverCommitWith(Map(), retry)
      val result = commit.handleErrorWith(recovery).attempt.unsafeRunSync()
      assert { result.left.toOption.map(_.getMessage).contains("commit") }
    }

    it("should have None as String representation") {
      assert(CommitRecovery.None.toString == "None")
    }
  }

  implicit val jitter: Jitter[IO] =
    Jitter.default[IO].unsafeRunSync()

  private def storeSleepsTimer(ref: Ref[IO, Chain[FiniteDuration]]): Temporal[IO] =
    new Temporal[IO] {
      override def clock: Clock[IO] = new Clock[IO] {
        override def realTime(unit: TimeUnit): IO[Long] =
          IO.raiseError(new RuntimeException("clock#realTime"))

        override def monotonic(unit: TimeUnit): IO[Long] =
          IO.raiseError(new RuntimeException("clock#monotonic"))
      }

      override def sleep(duration: FiniteDuration): IO[Unit] =
        ref.update(_ append duration)
    }
}

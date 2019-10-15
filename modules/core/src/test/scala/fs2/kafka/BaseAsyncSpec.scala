package fs2.kafka

import cats.effect.{ContextShift, IO, Timer}
import org.scalatest.Assertions
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpec

import scala.concurrent.ExecutionContext

abstract class BaseAsyncSpec extends AnyFunSpec with Assertions with Matchers {
  implicit val contextShift: ContextShift[IO] =
    IO.contextShift(ExecutionContext.global)

  implicit val timer: Timer[IO] =
    IO.timer(ExecutionContext.global)
}

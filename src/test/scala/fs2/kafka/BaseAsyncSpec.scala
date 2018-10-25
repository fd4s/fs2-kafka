package fs2.kafka

import cats.effect.{ContextShift, IO, Timer}
import org.scalatest.{Assertions, FunSpec, Matchers}

import scala.concurrent.ExecutionContext

abstract class BaseAsyncSpec extends FunSpec with Assertions with Matchers {
  implicit val contextShift: ContextShift[IO] =
    IO.contextShift(ExecutionContext.global)

  implicit val timer: Timer[IO] =
    IO.timer(ExecutionContext.global)
}

package fs2.kafka

import cats.effect.IO
import org.scalatest.Assertions
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpec

import scala.concurrent.ExecutionContext
import cats.effect.Temporal

abstract class BaseAsyncSpec extends AnyFunSpec with Assertions with Matchers {
  implicit val contextShift: ContextShift[IO] =
    IO.contextShift(ExecutionContext.global)

  implicit val timer: Temporal[IO] =
    IO.timer(ExecutionContext.global)
}

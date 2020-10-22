package fs2.kafka

import cats.effect.{IO}
import org.scalatest.Assertions
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpec

import scala.concurrent.ExecutionContext

abstract class BaseAsyncSpec extends AnyFunSpec with Assertions with Matchers {}

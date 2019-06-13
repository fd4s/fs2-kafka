package fs2.kafka

import org.scalatest._
import org.scalatestplus.scalacheck._
import org.scalatest.funspec.AnyFunSpec

abstract class BaseSpec
    extends AnyFunSpec
    with Assertions
    with Matchers
    with ScalaCheckPropertyChecks
    with BaseGenerators

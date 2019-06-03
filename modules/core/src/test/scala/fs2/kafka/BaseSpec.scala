package fs2.kafka

import org.scalatest._
import org.scalatestplus.scalacheck._

abstract class BaseSpec
    extends FunSpec
    with Assertions
    with Matchers
    with ScalaCheckPropertyChecks
    with BaseGenerators

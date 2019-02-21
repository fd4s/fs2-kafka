package fs2.kafka

import org.scalatest._
import org.scalatest.prop._

abstract class BaseSpec
    extends FunSpec
    with Assertions
    with Matchers
    with PropertyChecks
    with BaseGenerators

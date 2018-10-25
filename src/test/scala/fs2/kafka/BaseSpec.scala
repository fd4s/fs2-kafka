package fs2.kafka

import org.scalatest.prop.PropertyChecks
import org.scalatest.{Assertions, FunSpec}

abstract class BaseSpec extends FunSpec with Assertions with PropertyChecks with BaseGenerators

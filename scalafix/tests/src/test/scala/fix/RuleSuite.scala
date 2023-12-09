package fix

import org.scalatest.FunSuiteLike
import scalafix.testkit._

class RuleSuite extends AbstractSemanticRuleSuite with FunSuiteLike {
  runAllTests()
}

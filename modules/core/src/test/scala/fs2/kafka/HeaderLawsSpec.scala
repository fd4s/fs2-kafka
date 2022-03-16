package fs2.kafka

import cats.kernel.laws.discipline.EqTests

class HeaderLawsSpec extends BaseCatsSpec {
  checkAll(
    "Header.eqLaws",
    EqTests[Header].eqv
  )
}

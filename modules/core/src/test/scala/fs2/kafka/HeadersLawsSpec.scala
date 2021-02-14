package fs2.kafka

import cats.kernel.laws.discipline.EqTests

class HeadersLawsSpec extends BaseCatsSpec {

  checkAll(
    "Headers.eqLaws",
    EqTests[Headers].eqv
  )

}

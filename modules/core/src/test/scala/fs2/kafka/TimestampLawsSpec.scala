package fs2.kafka

import cats.kernel.laws.discipline.EqTests

class TimestampLawsSpec extends BaseCatsSpec {
  checkAll(
    "Timestamp.eqLaws",
    EqTests[Timestamp].eqv
  )
}

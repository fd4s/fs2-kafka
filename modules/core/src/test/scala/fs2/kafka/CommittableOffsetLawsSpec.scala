package fs2.kafka

import cats.effect.IO
import cats.kernel.laws.discipline.EqTests

class CommittableOffsetLawsSpec extends BaseCatsSpec {

  checkAll(
    "CommittableOffset.eqLaws",
    EqTests[CommittableOffset[IO]].eqv
  )

}

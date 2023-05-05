/*
 * Copyright 2018-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.kernel.laws.discipline.EqTests

class HeadersLawsSpec extends BaseCatsSpec {
  checkAll(
    "Headers.eqLaws",
    EqTests[Headers].eqv
  )
}

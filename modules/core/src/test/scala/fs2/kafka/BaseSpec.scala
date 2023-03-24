/*
 * Copyright 2018-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import org.scalatest._
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck._
import org.scalatest.funspec.AnyFunSpec

abstract class BaseSpec
    extends AnyFunSpec
    with Assertions
    with Matchers
    with ScalaCheckPropertyChecks
    with BaseGenerators

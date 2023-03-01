/*
 * Copyright 2018-2022 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import org.apache.kafka.common.KafkaException

sealed abstract class TransactionLeakedException
    extends KafkaException("transaction leak; the transaction has already been finalized")

private[kafka] object TransactionLeakedException {
  def apply(): TransactionLeakedException =
    new TransactionLeakedException {
      override def toString: String =
        s"fs2.kafka.TransactionLeakedException: $getMessage"
    }
}

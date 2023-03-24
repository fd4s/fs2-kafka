/*
 * Copyright 2018-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

private[kafka] object converters {
  val collection = scala.collection.JavaConverters

  def unsafeWrapArray[A](array: Array[A]): Seq[A] =
    array
}

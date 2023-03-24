/*
 * Copyright 2018-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

private[kafka] sealed abstract class LogLevel

private[kafka] object LogLevel {
  case object Error extends LogLevel
  case object Warn extends LogLevel
  case object Info extends LogLevel
  case object Debug extends LogLevel
}

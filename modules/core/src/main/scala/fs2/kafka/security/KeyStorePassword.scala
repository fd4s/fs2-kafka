/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.security

import cats.effect.Sync

import java.util.UUID

sealed abstract class KeyStorePassword {
  def value: String
}

private[security] object KeyStorePassword {
  def createTemporary[F[_]](implicit F: Sync[F]): F[KeyStorePassword] =
    F.delay {
      val _value = UUID.randomUUID().toString

      new KeyStorePassword {
        override final val value: String =
          _value

        override final def toString: String =
          s"KeyStorePassword(${value.valueShortHash})"
      }
    }
}

/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.security

import cats.effect.Sync
import cats.syntax.all._

import java.nio.file.Path

sealed abstract class TrustStoreFile {
  def path: Path
}

private[security] object TrustStoreFile {
  def createTemporary[F[_]](implicit F: Sync[F]): F[TrustStoreFile] =
    internal.FileOps.createTemp[F]("client.truststore-", ".jks").map { _path =>
      new TrustStoreFile {
        override final val path: Path = _path

        override final def toString: String =
          s"TrustStoreFile(${path.toString})"
      }
    }
}

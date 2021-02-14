/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.security.internal

import cats.effect.Sync

import java.nio.file.{Files, Path}

private[security] object FileOps {
  def createTemp[F[_]](prefix: String, suffix: String)(implicit F: Sync[F]): F[Path] = F.delay {
    val path = Files.createTempFile(prefix, suffix)
    path.toFile.deleteOnExit()
    Files.delete(path)
    path
  }
}

package fs2.kafka.security

import cats.effect.Sync
import cats.syntax.all._

import java.nio.file.Path

sealed abstract class KeyStoreFile {
  def path: Path

  def pathAsString: String
}

private[security] object KeyStoreFile {
  final def createTemporary[F[_]](implicit F: Sync[F]): F[KeyStoreFile] =
    internal.FileOps.createTemp("client.keystore-", ".p12").map { _path =>
      new KeyStoreFile {
        override final val path: Path =
          _path

        override final def pathAsString: String =
          path.toString

        override final def toString: String =
          s"KeyStoreFile($pathAsString)"
      }
    }
}

package fs2.kafka.security

import cats.effect.Sync
import cats.syntax.all._

import java.nio.file.Path

sealed abstract class TrustStoreFile {
  def path: Path

  def pathAsString: String
}

private[security] object TrustStoreFile {
  def createTemporary[F[_]](implicit F: Sync[F]): F[TrustStoreFile] =
    internal.FileOps.createTemp[F]("client.truststore-", ".jks").map { _path =>
      new TrustStoreFile {
        override final val path: Path =
          _path

        override final def pathAsString: String =
          path.toString

        override final def toString: String =
          s"TrustStoreFile($pathAsString)"
      }
    }
}

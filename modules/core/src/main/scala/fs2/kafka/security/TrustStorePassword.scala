package fs2.kafka.security

import cats.effect.Sync

import java.util.UUID

sealed abstract class TrustStorePassword {
  def value: String
}

private[security] object TrustStorePassword {
  def createTemporary[F[_]](implicit F: Sync[F]): F[TrustStorePassword] =
    F.delay {
      val _value = UUID.randomUUID().toString

      new TrustStorePassword {
        override final val value: String =
          _value

        override final def toString: String =
          s"TrustStorePassword(${value.valueShortHash})"
      }
    }
}

package fs2.kafka.security

import cats.syntax.all._

import java.nio.charset.StandardCharsets
import java.security.{GeneralSecurityException, KeyFactory, PrivateKey}
import java.security.spec.PKCS8EncodedKeySpec
import java.util.Base64

sealed abstract class ClientPrivateKey {
  def value: PrivateKey
}

object ClientPrivateKey {
  def fromString(clientPrivateKey: String): Either[GeneralSecurityException, ClientPrivateKey] = {
    Either.catchOnly[GeneralSecurityException] {
      new ClientPrivateKey {
        override final val value: PrivateKey =
          KeyFactory
            .getInstance("RSA")
            .generatePrivate {
              new PKCS8EncodedKeySpec(
                Base64.getDecoder.decode {
                  clientPrivateKey
                    .replace("-----BEGIN PRIVATE KEY-----", "")
                    .replace("-----END PRIVATE KEY-----", "")
                    .filterNot(_.isWhitespace)
                    .getBytes(StandardCharsets.UTF_8)
                }
              )
            }

        override final def toString: String =
          s"ClientPrivateKey(${clientPrivateKey.valueShortHash})"
      }
    }
  }

  def fromPrivateKey(privateKey: PrivateKey): ClientPrivateKey = {
    new ClientPrivateKey {
      override def value: PrivateKey = privateKey

      override final def toString: String =
        s"ClientPrivateKey(${privateKey.toString.valueShortHash})"
    }
  }
}

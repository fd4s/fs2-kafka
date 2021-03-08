/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.security

import cats.effect._
import cats.syntax.all._

import java.nio.file.{Files, Path}
import java.security.KeyStore

sealed trait KafkaCredentialStore {
  def properties: Map[String, String]
}

sealed abstract class Pkcs12KafkaCredentialStore extends KafkaCredentialStore {
  def keyStoreFile: KeyStoreFile

  def keyStorePassword: KeyStorePassword

  def trustStoreFile: TrustStoreFile

  def trustStorePassword: TrustStorePassword
}

object Pkcs12KafkaCredentialStore {
  final def apply[F[_]: ContextShift](
    clientPrivateKey: ClientPrivateKey,
    clientCertificate: ClientCertificate,
    serviceCertificate: ServiceCertificate,
    blocker: Blocker
  )(implicit F: Sync[F]): F[Pkcs12KafkaCredentialStore] =
    for {
      setupDetails <- Pkcs12KafkaCredentialStore.createTemporary[F]
      _ <- setupKeyStore(
        clientPrivateKey = clientPrivateKey,
        clientCertificate = clientCertificate,
        keyStoreFile = setupDetails.keyStoreFile,
        keyStorePassword = setupDetails.keyStorePassword,
        blocker = blocker
      )
      _ <- setupTrustStore(
        serviceCertificate = serviceCertificate,
        trustStoreFile = setupDetails.trustStoreFile,
        trustStorePassword = setupDetails.trustStorePassword,
        blocker = blocker
      )
    } yield setupDetails

  final def createFromStrings[F[_]: Sync: ContextShift](
    clientPrivateKey: String,
    clientCertificate: String,
    serviceCertificate: String,
    blocker: Blocker
  ): F[Pkcs12KafkaCredentialStore] =
    (
      ClientPrivateKey.fromString(clientPrivateKey).liftTo[F],
      ClientCertificate.fromString(clientCertificate).liftTo[F],
      ServiceCertificate.fromString(serviceCertificate).liftTo[F]
    ).tupled.flatMap {
      case (clientPrivateKey, clientCertificate, serviceCertificate) =>
        apply[F](clientPrivateKey, clientCertificate, serviceCertificate, blocker)
    }

  private final def setupStore[F[_]: ContextShift](
    storeType: String,
    storePath: Path,
    storePasswordChars: Array[Char],
    setupStore: KeyStore => Unit,
    blocker: Blocker
  )(implicit F: Sync[F]): F[Unit] =
    blocker.delay {
      val keyStore = KeyStore.getInstance(storeType)
      keyStore.load(null, storePasswordChars)
      setupStore(keyStore)

      val outputStream = Files.newOutputStream(storePath)

      try {
        keyStore.store(outputStream, storePasswordChars)
      } finally {
        outputStream.close()
      }
    }

  private final def setupKeyStore[F[_]: ContextShift](
    clientPrivateKey: ClientPrivateKey,
    clientCertificate: ClientCertificate,
    keyStoreFile: KeyStoreFile,
    keyStorePassword: KeyStorePassword,
    blocker: Blocker
  )(implicit F: Sync[F]): F[Unit] = {
    val keyStorePasswordChars =
      keyStorePassword.value.toCharArray

    setupStore(
      storeType = "PKCS12",
      storePath = keyStoreFile.path,
      storePasswordChars = keyStorePasswordChars,
      setupStore = _.setEntry(
        "service_key",
        new KeyStore.PrivateKeyEntry(
          clientPrivateKey.value,
          Array(clientCertificate.value)
        ),
        new KeyStore.PasswordProtection(keyStorePasswordChars)
      ),
      blocker = blocker
    )
  }

  private final def setupTrustStore[F[_]: ContextShift](
    serviceCertificate: ServiceCertificate,
    trustStoreFile: TrustStoreFile,
    trustStorePassword: TrustStorePassword,
    blocker: Blocker
  )(implicit F: Sync[F]): F[Unit] =
    setupStore(
      storeType = "JKS",
      storePath = trustStoreFile.path,
      storePasswordChars = trustStorePassword.value.toCharArray,
      setupStore = _.setCertificateEntry("CA", serviceCertificate.value),
      blocker = blocker
    )

  private final def createTemporary[F[_]](implicit F: Sync[F]): F[Pkcs12KafkaCredentialStore] =
    for {
      _keyStoreFile <- KeyStoreFile.createTemporary[F]
      _keyStorePassword <- KeyStorePassword.createTemporary[F]
      _trustStoreFile <- TrustStoreFile.createTemporary[F]
      _trustStorePassword <- TrustStorePassword.createTemporary[F]
    } yield {
      new Pkcs12KafkaCredentialStore {
        override final val keyStoreFile: KeyStoreFile =
          _keyStoreFile

        override final val keyStorePassword: KeyStorePassword =
          _keyStorePassword

        override final val trustStoreFile: TrustStoreFile =
          _trustStoreFile

        override final val trustStorePassword: TrustStorePassword =
          _trustStorePassword

        override final val properties: Map[String, String] =
          Map(
            "security.protocol" -> "SSL",
            "ssl.truststore.location" -> trustStoreFile.path.toString,
            "ssl.truststore.password" -> trustStorePassword.value,
            "ssl.keystore.type" -> "PKCS12",
            "ssl.keystore.location" -> keyStoreFile.path.toString,
            "ssl.keystore.password" -> keyStorePassword.value,
            "ssl.key.password" -> keyStorePassword.value
          )

        override final def toString: String =
          s"KafkaCredentialStore($keyStoreFile, $keyStorePassword, $trustStoreFile, $trustStorePassword)"
      }
    }
}

sealed abstract class PemKafkaCredentialStore extends KafkaCredentialStore

object PemKafkaCredentialStore {
  final def createFromStrings[F[_]: Sync: ContextShift](
    caCertificate: String,
    accessKey: String,
    accessCertificate: String
  ): PemKafkaCredentialStore =
    new PemKafkaCredentialStore {
      override def properties: Map[String, String] =
        Map(
          "security.protocol" -> "SSL",
          "ssl.truststore.type" -> "PEM",
          "ssl.truststore.certificates" -> caCertificate,
          "ssl.keystore.type" -> "PEM",
          "ssl.keystore.key" -> accessKey,
          "ssl.keystore.certificate.chain" -> accessCertificate
        )
    }
}

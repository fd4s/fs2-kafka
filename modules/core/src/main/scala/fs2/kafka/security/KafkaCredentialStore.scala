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

sealed abstract class KafkaCredentialStore {
  def keyStoreFile: KeyStoreFile

  def keyStorePassword: KeyStorePassword

  def trustStoreFile: TrustStoreFile

  def trustStorePassword: TrustStorePassword

  def properties: Map[String, String]
}

object KafkaCredentialStore {
  final def apply[F[_]: ContextShift](
    clientPrivateKey: ClientPrivateKey,
    clientCertificate: ClientCertificate,
    serviceCertificate: ServiceCertificate,
    blocker: Blocker
  )(implicit F: Sync[F]): F[KafkaCredentialStore] =
    for {
      setupDetails <- KafkaCredentialStore.createTemporary[F]
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

  private final def createTemporary[F[_]](implicit F: Sync[F]): F[KafkaCredentialStore] =
    for {
      _keyStoreFile <- KeyStoreFile.createTemporary[F]
      _keyStorePassword <- KeyStorePassword.createTemporary[F]
      _trustStoreFile <- TrustStoreFile.createTemporary[F]
      _trustStorePassword <- TrustStorePassword.createTemporary[F]
    } yield {
      new KafkaCredentialStore {
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

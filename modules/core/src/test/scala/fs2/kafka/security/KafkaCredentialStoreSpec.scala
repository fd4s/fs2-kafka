/*
 * Copyright 2018-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.security

import fs2.kafka.BaseSpec

final class KafkaCredentialStoreSpec extends BaseSpec {
  describe("KafkaCredentialStore") {
    describe("fromPemStrigs") {
      it("should create a KafkaCredentialStore with the expected properties") {
        val caCert =
          """-----BEGIN CERTIFICATE-----
            |RmFrZSBDQSBjZXJ0aWZpY2F0ZSBGYWtlIENBIGNlcnRpZmljYXRlIEZha2UgQ0EgY2VydGlmaWNh
            |dGUgRmFrZSBDQSBjZXJ0aWZpY2F0ZQ==
            |-----END CERTIFICATE-----""".stripMargin

        val privateKey =
          """-----BEGIN PRIVATE KEY-----
            |RmFrZSBwcml2YXRlIGtleSBGYWtlIHByaXZhdGUga2V5IEZha2UgcHJpdmF0ZSBrZXkgRmFrZSBw
            |cml2YXRlIGtleSBGYWtlIHByaXZhdGUga2V5IA==
            |-----END PRIVATE KEY-----""".stripMargin

        val clientCert =
          """-----BEGIN CERTIFICATE-----
            |RmFrZSBjbGllbnQgY2VydCBGYWtlIGNsaWVudCBjZXJ0IEZha2UgY2xpZW50IGNlcnQgRmFrZSBj
            |bGllbnQgY2VydCBGYWtlIGNsaWVudCBjZXJ0IA==
            |-----END CERTIFICATE-----""".stripMargin

        val store = KafkaCredentialStore.fromPemStrings(caCert, privateKey, clientCert)

        assert(
          store.properties === Map(
            "security.protocol" -> "SSL",
            "ssl.truststore.type" -> "PEM",
            "ssl.truststore.certificates" -> "-----BEGIN CERTIFICATE----- RmFrZSBDQSBjZXJ0aWZpY2F0ZSBGYWtlIENBIGNlcnRpZmljYXRlIEZha2UgQ0EgY2VydGlmaWNh dGUgRmFrZSBDQSBjZXJ0aWZpY2F0ZQ== -----END CERTIFICATE-----",
            "ssl.keystore.type" -> "PEM",
            "ssl.keystore.key" -> "-----BEGIN PRIVATE KEY----- RmFrZSBwcml2YXRlIGtleSBGYWtlIHByaXZhdGUga2V5IEZha2UgcHJpdmF0ZSBrZXkgRmFrZSBw cml2YXRlIGtleSBGYWtlIHByaXZhdGUga2V5IA== -----END PRIVATE KEY-----",
            "ssl.keystore.certificate.chain" -> "-----BEGIN CERTIFICATE----- RmFrZSBjbGllbnQgY2VydCBGYWtlIGNsaWVudCBjZXJ0IEZha2UgY2xpZW50IGNlcnQgRmFrZSBj bGllbnQgY2VydCBGYWtlIGNsaWVudCBjZXJ0IA== -----END CERTIFICATE-----"
          )
        )
      }
    }
  }
}

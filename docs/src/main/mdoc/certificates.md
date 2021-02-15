---
id: security
title: Security & Certificates
---

## Security: certificates, trust stores, and passwords 

The `KafkaCredentialStore` can be used to create the necessary trust stores and passwords to access kafka.

The parameters passed in are string representations of the client private key, client certificate
and service certificate. the `properties` field in `KafkaCredentialStore` can then be applied to
any of the `*Settings` classes by using the `withProperties(kafkaCredentialStore.properties)`.

```scala mdoc
import cats.effect._
import cats.syntax.all._
import fs2.kafka.security._

def createKafkaProducer[F[_]: Sync: ContextShift](
    clientPrivateKey: String,
    clientCertificate: String,
    serviceCertificate: String,
): F[ProducerSettings[F, UUID, String]] =
  Blocker[F].use { blocker =>
    KafkaCredentialStore.createFromStrings[F](clientPrivateKey, clientCertificate, serviceCertificate, blocker)
  }.map { credentialStore =>
    ProducerSettings(Serializer.uuid[F], Serializer.string[F])
      .withProperties(credentialStore.properties)
  }
```

---
id: security
title: Security & Certificates
---

## Security: certificates, trust stores, and passwords 

The `KafkaCredentialStore` can be used to create the necessary.

The parameters passed in are string representations of the client private key, client certificate
and service certificate. the `properties` field in `KafkaCredentialStore` can then be applied to
any of the `*Settings` classes by using the `withProperties(kafkaCredentialStore.properties)`.

```scala mdoc
import cats.effect._
import cats.syntax.all._
import fs2.kafka.security._

def loadKafkaSetup[F[_]: Async: ContextShift](
    clientPrivateKey: String,
    clientCertificate: String,
    serviceCertificate: String,
): F[KafkaCredentialStore] =
  Blocker[F].use { blocker =>
    (
      ClientPrivateKey.fromString(clientPrivateKey).liftTo[F],
      ClientCertificate.fromString(clientCertificate).liftTo[F],
      ServiceCertificate.fromString(serviceCertificate).liftTo[F],
    ).tupled.flatMap {
      case (clientPrivateKey, clientCertificate, serviceCertificate) =>
        KafkaCredentialStore[F](clientPrivateKey, clientCertificate, serviceCertificate, blocker)
    }
  }
```

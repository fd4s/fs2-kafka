/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.security

import java.security.cert.{Certificate, CertificateException}

sealed abstract class ClientCertificate {
  def value: Certificate
}

object ClientCertificate {
  def fromString(clientCertificate: String): Either[CertificateException, ClientCertificate] =
    internal.CertificateOps.loadFromString(clientCertificate).map { certificate =>
      new ClientCertificate {
        override final val value: Certificate =
          certificate

        override final def toString: String =
          s"ClientCertificate(${clientCertificate.valueShortHash})"
      }
    }

  def fromCertificate(certificate: Certificate): ClientCertificate =
    new ClientCertificate {
      override def value: Certificate = certificate
    }
}

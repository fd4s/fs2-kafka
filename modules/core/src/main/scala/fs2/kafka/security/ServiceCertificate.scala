/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.security

import java.security.cert.{Certificate, CertificateException}

sealed abstract class ServiceCertificate {
  def value: Certificate
}

object ServiceCertificate {
  def fromString(serviceCertificate: String): Either[CertificateException, ServiceCertificate] = {
    internal.CertificateOps.loadFromString(serviceCertificate).map { certificate =>
      new ServiceCertificate {
        override final val value: Certificate =
          certificate

        override final def toString: String =
          s"ServiceCertificate(${serviceCertificate.valueShortHash})"
      }
    }
  }

  def fromCertificate(certificate: Certificate): ServiceCertificate = {
    new ServiceCertificate {
      override def value: Certificate = certificate
    }
  }
}

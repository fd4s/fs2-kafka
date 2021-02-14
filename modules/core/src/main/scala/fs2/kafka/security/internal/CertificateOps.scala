package fs2.kafka.security.internal

import cats.syntax.all._

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.security.cert.{Certificate, CertificateException, CertificateFactory}

private[security] object CertificateOps {
  def loadFromString(certificate: String): Either[CertificateException, Certificate] =
    loadFromBytes(certificate.getBytes(StandardCharsets.UTF_8))

  def loadFromBytes(certificateBytes: Array[Byte]): Either[CertificateException, Certificate] =
    Either.catchOnly[CertificateException] {
      CertificateFactory
        .getInstance("X.509")
        .generateCertificate(new ByteArrayInputStream(certificateBytes))
    }
}

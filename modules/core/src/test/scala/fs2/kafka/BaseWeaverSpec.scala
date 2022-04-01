/*
This file contains code derived from the Embedded Kafka library
(https://github.com/embeddedkafka/embedded-kafka), the license for which is reproduced below.

   The MIT License (MIT)

   Copyright (c) 2016 Emanuele Blanco

   Permission is hereby granted, free of charge, to any person obtaining a copy
   of this software and associated documentation files (the "Software"), to deal
   in the Software without restriction, including without limitation the rights
   to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
   copies of the Software, and to permit persons to whom the Software is
   furnished to do so, subject to the following conditions:

   The above copyright notice and this permission notice shall be included in all
   copies or substantial portions of the Software.

   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
   IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
   FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
   AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
   LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
   OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
   SOFTWARE.
 */
package fs2.kafka

import cats.effect.{IO, Resource}
import com.dimafeng.testcontainers.KafkaContainer
import weaver.{GlobalResource, GlobalWrite, IOSuite}

import scala.concurrent.duration.{DurationInt, FiniteDuration}

object BaseWeaverSpecShared extends GlobalResource {
  protected val imageVersion = "7.0.1"

  protected val imageName = Option(System.getProperty("os.arch")) match {
    case Some("aarch64") =>
      "niciqy/cp-kafka-arm64" // no official docker image for ARM is available yet
    case _ => "confluentinc/cp-kafka"
  }
  final val transactionTimeoutInterval: FiniteDuration = 1.second

  lazy val container: KafkaContainer = new KafkaContainer()
    .configure { container =>
      container
        .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .withEnv(
          "KAFKA_TRANSACTION_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS",
          transactionTimeoutInterval.toMillis.toString
        )
        .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .withEnv("KAFKA_AUTHORIZER_CLASS_NAME", "kafka.security.authorizer.AclAuthorizer")
        .withEnv("KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND", "true")
        .setDockerImageName(s"$imageName:$imageVersion")

      ()
    }

  override def sharedResources(global: GlobalWrite): Resource[IO, Unit] =
    ContainerResource(IO(container)).evalMap(global.put(_))
}

abstract class BaseWeaverSpec extends IOSuite with BaseKafkaSpecBase {

  override type Res = KafkaContainer

  override def sharedResource: Resource[IO, KafkaContainer] = ContainerResource(IO(container))

}

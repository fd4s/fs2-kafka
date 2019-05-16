/*
 * Copyright 2018-2019 OVO Energy Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fs2.kafka.internal

import cats.effect._
import cats.implicits._
import fs2.kafka._
import fs2.kafka.internal.syntax._
import scala.concurrent.ExecutionContext

private[kafka] sealed abstract class WithProducer[F[_]] {
  def apply[A](f: KafkaByteProducer => F[A]): F[A]
}

private[kafka] object WithProducer {
  def apply[F[_], K, V](
    settings: ProducerSettings[F, K, V]
  )(
    implicit F: Sync[F],
    context: ContextShift[F]
  ): Resource[F, WithProducer[F]] = {
    val executionContextResource =
      settings.executionContext
        .map(Resource.pure[F, ExecutionContext])
        .getOrElse(producerExecutionContextResource)

    executionContextResource.flatMap { executionContext =>
      Resource.make[F, WithProducer[F]] {
        settings.createProducer
          .map { producer =>
            new WithProducer[F] {
              override def apply[A](f: KafkaByteProducer => F[A]): F[A] =
                context.evalOn(executionContext) {
                  f(producer)
                }
            }
          }
      } { withProducer =>
        withProducer { producer =>
          F.delay(producer.close(settings.closeTimeout.asJava))
        }
      }
    }
  }
}

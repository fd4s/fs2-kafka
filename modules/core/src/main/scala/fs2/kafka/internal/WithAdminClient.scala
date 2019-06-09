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
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.KafkaFuture
import scala.concurrent.ExecutionContext

private[kafka] sealed abstract class WithAdminClient[F[_]] {
  def apply[A](f: AdminClient => KafkaFuture[A]): F[A]

  def close: F[Unit]
}

private[kafka] object WithAdminClient {
  def apply[F[_]](
    settings: AdminClientSettings[F]
  )(
    implicit F: Concurrent[F],
    context: ContextShift[F]
  ): Resource[F, WithAdminClient[F]] = {
    val executionContextResource =
      settings.executionContext
        .map(Resource.pure[F, ExecutionContext])
        .getOrElse(ExecutionContexts.adminClient)

    executionContextResource.flatMap { executionContext =>
      Resource.make[F, WithAdminClient[F]] {
        settings.createAdminClient
          .map { adminClient =>
            new WithAdminClient[F] {
              override def apply[A](f: AdminClient => KafkaFuture[A]): F[A] =
                context.evalOn(executionContext) {
                  F.suspend(f(adminClient).cancelable)
                }

              override def close: F[Unit] =
                context.evalOn(executionContext) {
                  F.delay(adminClient.close(settings.closeTimeout.asJava))
                }
            }
          }
      }(_.close)
    }
  }
}

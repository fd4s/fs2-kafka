/*
 * Copyright 2018-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.admin

import cats.effect.Sync
import fs2.kafka.AdminClientSettings
import fs2.kafka.consumer.MkConsumer
import org.apache.kafka.clients.admin.AdminClient
import fs2.kafka.internal.converters.collection._

/**
  * A capability trait representing the ability to instantiate the Java
  * `AdminClient` that underlies the fs2-kafka `KafkaAdminClient`. This
  * is needed in order to instantiate [[fs2.kafka.KafkaAdminClient]].
  *
  * By default, the instance provided by [[MkAdminClient.mkAdminClientForSync]]
  * will be used. However this behaviour can be overridden, e.g. for
  * testing purposes, by placing an alternative implicit instance in
  * lexical scope.
  */
trait MkAdminClient[F[_]] {
  def apply(settings: AdminClientSettings): F[AdminClient]
}

object MkAdminClient {
  implicit def mkAdminClientForSync[F[_]](implicit F: Sync[F]): MkAdminClient[F] =
    settings =>
      F.blocking {
        AdminClient.create {
          (settings.properties: Map[String, AnyRef]).asJava
        }
      }
}
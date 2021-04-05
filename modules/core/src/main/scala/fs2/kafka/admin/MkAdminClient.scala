package fs2.kafka.admin

import cats.effect.Sync
import fs2.kafka.AdminClientSettings
import org.apache.kafka.clients.admin.AdminClient
import fs2.kafka.internal.converters.collection._

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

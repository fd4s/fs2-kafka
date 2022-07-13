package fs2.kafka

import cats.effect.{Resource, Sync}
import cats.syntax.all._
import com.dimafeng.testcontainers.Container

object ContainerResource {
  def apply[F[_], C <: Container](container: F[C])(implicit F: Sync[F]): Resource[F, C] =
    Resource.make(container.flatTap { container =>
      F.blocking(container.start())
    })(c => F.blocking(c.stop()))
}

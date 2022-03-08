package fs2.kafka

import cats.effect.{Blocker, ContextShift, Resource, Sync}

package object internal {
  type ExclusiveAccess[F[_], A] = F[A] => F[A]

  private[kafka] def blockingResource[F[_]: Sync: ContextShift](
    settings: ProducerSettings[F, _, _]
  ): Resource[F, Blocking[F]] =
    settings.blocker
      .map(Resource.pure[F, Blocker])
      .getOrElse(Blockers.producer)
      .map(Blocking.fromBlocker[F])
}

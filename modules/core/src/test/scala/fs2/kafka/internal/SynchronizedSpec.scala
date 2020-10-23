package fs2.kafka.internal

import cats.effect.{MVar, Ref}
import cats.effect.{Fiber, IO}
import cats.implicits._
import fs2.kafka.BaseAsyncSpec

final class SynchronizedSpec extends BaseAsyncSpec {
  it("should synchronize concurrent access to the resource") {
    val success =
      (for {
        success <- Ref[IO].of(true)
        resource <- MVar[IO].of("resource")
        synch <- Synchronized[IO].of(resource)
        use = (n: Int) =>
          IO.shift >> synch
            .use { mVar =>
              mVar.tryTake.flatMap { takenOption =>
                takenOption
                  .map(mVar.put(_) >> {
                    if (n % 50 == 0)
                      IO.raiseError(new RuntimeException)
                    else IO.unit
                  })
                  .getOrElse(success.set(false))
              }
            }
            .attempt
            .start
        uses <- (1 to 1000).toList.map(use).combineAll
        _ <- uses.join
        succeeded <- success.get
      } yield succeeded).unsafeRunSync()

    assert(success)
  }

  it("should continue to work when use is cancelled") {
    val success =
      (for {
        synch <- Synchronized[IO].of("resource")
        fiberUnit = Fiber(IO.unit, IO.unit)
        use = (n: Int) =>
          IO.shift >> synch
            .use(_ => IO.unit)
            .start
            .flatMap { fiber =>
              if (n % 50 == 0)
                fiber.cancel.as(fiberUnit)
              else IO.pure(fiber)
            }
        used <- (1 to 1000).toList.map(use).combineAll
        _ <- used.join
      } yield true).unsafeRunSync()

    assert(success)
  }
}

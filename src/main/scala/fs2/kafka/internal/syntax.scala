package fs2.kafka.internal

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration

private[kafka] object syntax {
  implicit final class FiniteDurationSyntax(val duration: FiniteDuration) extends AnyVal {
    def asJava: Duration =
      if (duration.length == 0L) Duration.ZERO
      else
        duration.unit match {
          case TimeUnit.DAYS         => Duration.ofDays(duration.length)
          case TimeUnit.HOURS        => Duration.ofHours(duration.length)
          case TimeUnit.MINUTES      => Duration.ofMinutes(duration.length)
          case TimeUnit.SECONDS      => Duration.ofSeconds(duration.length)
          case TimeUnit.MILLISECONDS => Duration.ofMillis(duration.length)
          case TimeUnit.MICROSECONDS => Duration.of(duration.length, ChronoUnit.MICROS)
          case TimeUnit.NANOSECONDS  => Duration.ofNanos(duration.length)
        }
  }
}

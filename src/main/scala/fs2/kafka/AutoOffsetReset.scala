package fs2.kafka

sealed abstract class AutoOffsetReset

object AutoOffsetReset {
  private[kafka] case object EarliestOffsetReset extends AutoOffsetReset

  private[kafka] case object LatestOffsetReset extends AutoOffsetReset

  private[kafka] case object NoOffsetReset extends AutoOffsetReset

  val Earliest: AutoOffsetReset = EarliestOffsetReset

  val Latest: AutoOffsetReset = LatestOffsetReset

  val None: AutoOffsetReset = NoOffsetReset
}

package fs2.kafka

sealed abstract class Acks

object Acks {
  private[kafka] case object ZeroAcks extends Acks

  private[kafka] case object OneAcks extends Acks

  private[kafka] case object AllAcks extends Acks

  val Zero: Acks = ZeroAcks

  val One: Acks = OneAcks

  val All: Acks = AllAcks
}

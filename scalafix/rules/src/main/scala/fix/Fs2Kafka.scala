package fix

import scalafix.v1._
import scala.meta._

class Fs2Kafka extends SemanticRule("Fs2Kafka") {
  override def fix(implicit doc: SemanticDocument): Patch =
    reorderPassthroughParams

  def reorderPassthroughParams(implicit doc: SemanticDocument): Patch = {
    val ProducerRecords_M =
      SymbolMatcher.normalized(
        "fs2/kafka/ProducerRecords.",
        "fs2/kafka/ProducerRecords#apply."
      )

    val ProducerRecords_one_M =
      SymbolMatcher.normalized("fs2/kafka/ProducerRecords.one.")

    val ProducerResult_M =
      SymbolMatcher.normalized(
        "fs2/kafka/ProducerResult.",
        "fs2/kafka/ProducerResult#apply."
      )

    val TransactionalProducerRecords_M =
      SymbolMatcher.normalized(
        "fs2/kafka/TransactionalProducerRecords.",
        "fs2/kafka/TransactionalProducerRecords#apply."
      )

    val TransactionalProducerRecords_one_M =
      SymbolMatcher.normalized("fs2/kafka/TransactionalProducerRecords.one.")

    doc.tree.collect {
      // ProducerRecords[K, V, P] -> ProducerRecords[P, K, V]
      case term @ Type.Apply(ProducerRecords_M(fun), List(k, v, p)) =>
        Patch.replaceTree(term, s"${fun.syntax}[$p, $k, $v]")
      // ProducerRecords[F, K, V, P] -> ProducerRecords[F, P, K, V]
      case term @ Term.ApplyType(ProducerRecords_M(fun), List(f, k, v, p)) =>
        Patch.replaceTree(term, s"${fun.syntax}[$f, $p, $k, $v]")
      // ProducerRecords.one[K, V, P] -> ProducerRecords.one[P, K, V]
      case term @ Term.ApplyType(ProducerRecords_one_M(fun), List(k, v, p)) =>
        Patch.replaceTree(term, s"${fun.syntax}[$p, $k, $v]")
      // ProducerResult[K, V, P] -> ProducerResult[P, K, V]
      case term @ Type.Apply(ProducerResult_M(fun), List(k, v, p)) =>
        Patch.replaceTree(term, s"${fun.syntax}[$p, $k, $v]")
      case term @ Term.ApplyType(ProducerResult_M(fun), List(k, v, p)) =>
        Patch.replaceTree(term, s"${fun.syntax}[$p, $k, $v]")
      // TransactionalProducerResult[F, K, V, P] -> TransactionalProducerResult[F, P, K, V]
      case term @ Type.Apply(
            TransactionalProducerRecords_M(fun),
            List(f, k, v, p)
          ) =>
        Patch.replaceTree(term, s"${fun.syntax}[$f, $p, $k, $v]")
      case term @ Term.ApplyType(
            TransactionalProducerRecords_M(fun),
            List(f, k, v, p)
          ) =>
        Patch.replaceTree(term, s"${fun.syntax}[$f, $p, $k, $v]")
      // TransactionalProducerRecords.one[F, K, V, P] -> TransactionalProducerRecords.one[F, P, K, V]
      case term @ Term.ApplyType(
            TransactionalProducerRecords_one_M(fun),
            List(f, k, v, p)
          ) =>
        Patch.replaceTree(term, s"${fun.syntax}[$f, $p, $k, $v]")
    }.asPatch
  }
}

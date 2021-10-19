package fix

import scalafix.v1._
import scala.meta._

class Fs2Kafka extends SemanticRule("Fs2Kafka") {
  val packageObjectDeprecations = Map(
    "consumerResource" -> ("KafkaConsumer", "resource"),
    "consumerStream" -> ("KafkaConsumer", "stream"),
    "adminClientResource" -> ("KafkaAdminClient", "resource"),
    "adminClientStream" -> ("KafkaAdminClient", "stream"),
    "producerResource" -> ("KafkaProducer", "resource"),
    "producerStream" -> ("KafkaProducer", "stream"),
    "transactionalProducerResource" -> ("TransactionalKafkaProducer", "resource"),
    "transactionalProducerStream" -> ("TransactionalKafkaProducer", "stream"),
    "produce" -> ("KafkaProducer", "pipe")
  )

  val PartiallyApplied_package_M = SymbolMatcher.exact(
    "fs2/kafka/package.consumerStream(+1).",
    "fs2/kafka/package.consumerResource(+1).",
    "fs2/kafka/package.producerStream(+1).",
    "fs2/kafka/package.producerResource(+1).",
    "fs2/kafka/package.transactionalProducerStream(+1).",
    "fs2/kafka/package.transactionalProducerResource(+1)."
  )

  val Companion_M = SymbolMatcher.exact(
    "fs2/kafka/KafkaConsumer.",
    "fs2/kafka/KafkaProducer.",
    "fs2/kafka/TransactionalKafkaProducer."
  )

  override def fix(implicit doc: SemanticDocument): Patch =
    doc.tree.collect {
      case term @ Term.Select(
            Term.ApplyType(
              PartiallyApplied_package_M(fun),
              List(f)
            ),
            Term.Name("using")
          ) =>
        val (obj, method) =
          packageObjectDeprecations(fun.symbol.displayName.split('.').last)
        Patch.replaceTree(term, s"$obj[$f].$method") + Patch.addGlobalImport(
          Symbol(s"fs2/kafka/$obj#")
        )
      case term @ Term.Select(
            Term.ApplyType(
              Term.Select(Companion_M(companion), Term.Name(method)),
              List(f)
            ),
            Term.Name("using")
          ) =>
        Patch.replaceTree(term, s"${companion.syntax}[$f].$method")
      case term @ Term.Name(name)
          if term.symbol.owner == Symbol(
            "fs2/kafka/package."
          ) && !term.symbol.value.contains("(+1)") =>
        packageObjectDeprecations
          .get(name)
          .map {
            case (obj, method) =>
              Patch.replaceTree(term, s"$obj.$method") +
                Patch.addGlobalImport(Symbol(s"fs2/kafka/$obj#"))
          }
          .getOrElse(Patch.empty)
    }.asPatch
}

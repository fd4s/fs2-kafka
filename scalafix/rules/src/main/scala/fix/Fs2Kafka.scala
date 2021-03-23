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

  override def fix(implicit doc: SemanticDocument): Patch =
    doc.tree.collect {
      case term @ Term.Name(name)
          if term.symbol.owner == Symbol("fs2/kafka/package.") =>
        packageObjectDeprecations
          .get(name)
          .map { case (obj, method) =>
            Patch.replaceTree(term, s"$obj.$method") +
              Patch.addGlobalImport(Symbol(s"fs2/kafka/$obj#"))
          }
          .getOrElse(Patch.empty)
    }.asPatch
}

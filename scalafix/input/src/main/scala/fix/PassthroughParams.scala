/*
rule = Fs2Kafka
 */

package fix

import cats.effect.IO
import cats.syntax.all._
import fs2.kafka._
import fs2.Chunk

class PassthroughParams {

  val records: ProducerRecords[String, String, Int] =
    ProducerRecords.one[String, String, Int](
      ProducerRecord("topic", "key", "value"),
      42
    )

  fs2.kafka.ProducerRecords.one[String, String, Int](ProducerRecord("topic", "key", "value"), 42)

  ProducerRecords[List, String, String, Int](
    List(
      ProducerRecord("topic", "key", "value"),
      ProducerRecord("topic", "key2", "value2")
    ),
    42
  )

  ProducerRecords.apply[List, String, String, Int](
    List(),
    42
  )

  val result: ProducerResult[String, String, Int] =
    ProducerResult[String, String, Int](Chunk.empty, 42)

  val tRecords: TransactionalProducerRecords[IO, String, String, Int] =
    TransactionalProducerRecords[IO, String, String, Int](Chunk.empty, 42)

  TransactionalProducerRecords.one[IO, String, String, Int](???, 42)

}

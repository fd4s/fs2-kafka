package fix

import fs2.kafka._
import cats.implicits._
import fs2.Chunk
import cats.effect.IO

class PassthroughParams {
  val records: ProducerRecords[Int, String, String] =
    ProducerRecords.one[Int, String, String](
      ProducerRecord("topic", "key", "value"),
      42
    )

  fs2.kafka.ProducerRecords
    .one[Int, String, String](ProducerRecord("topic", "key", "value"), 42)

  ProducerRecords[List, Int, String, String](
    List(
      ProducerRecord("topic", "key", "value"),
      ProducerRecord("topic", "key2", "value2")
    ),
    42
  )

  ProducerRecords.apply[List, Int, String, String](
    List(),
    42
  )

  val result: ProducerResult[Int, String, String] =
    ProducerResult[Int, String, String](Chunk.empty, 42)

  val tRecords: TransactionalProducerRecords[IO, Int, String, String] =
    TransactionalProducerRecords[IO, Int, String, String](Chunk.empty, 42)

  TransactionalProducerRecords.one[IO, Int, String, String](???, 42)
}

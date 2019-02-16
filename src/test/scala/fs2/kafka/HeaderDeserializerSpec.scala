package fs2.kafka

import cats.laws.discipline._

final class HeaderDeserializerSpec extends BaseCatsSpec {
  checkAll("HeaderDeserializer", MonadTests[HeaderDeserializer].monad[String, String, String])

  test("HeaderDeserializer#delay") {
    var deserialized = false

    val deserializer =
      HeaderDeserializer.instance { bytes =>
        deserialized = true
        bytes
      }.delay

    val eval = deserializer.deserialize(Array())
    assert(!deserialized)

    val result = eval.value
    assert(deserialized)
    assert(result.isEmpty)
  }

  test("HeaderDeserializer#toString") {
    assert(HeaderDeserializer[String].toString startsWith "HeaderDeserializer$")
  }
}

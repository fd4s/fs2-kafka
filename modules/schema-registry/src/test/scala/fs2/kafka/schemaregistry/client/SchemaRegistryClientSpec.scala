package fs2.kafka.schemaregistry.client

import cats.effect.IO
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference
import org.scalatest.funspec.AnyFunSpec
import org.scalatestplus.scalacheck._

import java.util

final class SchemaRegistryClientSpec extends AnyFunSpec with ScalaCheckPropertyChecks {

  import cats.effect.unsafe.implicits._

  describe("SchemaRegistryClient") {

    it("should provide register") {
      assert {
        SchemaRegistryClient
          .fromJava[IO](new MockSchemaRegistryClient())
          .register("subject", StubbedParsedSchema("schema1"))
          .unsafeRunSync() == 1
      }
    }

    it("should provide getLatestSchemaMetadata") {
      assert {
        SchemaRegistryClient
          .fromJava[IO](new MockSchemaRegistryClient())
          .getLatestSchemaMetadata("subject")
          .attempt
          .unsafeRunSync()
          .isLeft
      }
    }

    it("should provide getSchemaMetadata") {
      assert {
        SchemaRegistryClient
          .fromJava[IO](new MockSchemaRegistryClient())
          .getSchemaMetadata("subject", 1)
          .attempt
          .unsafeRunSync()
          .isLeft
      }
    }

    it("should provide getSchemaById") {
      val client = SchemaRegistryClient.fromJava[IO](new MockSchemaRegistryClient())
      val schema = StubbedParsedSchema("schema1")
      val test = for {
        id <- client.register("subject", schema)
        schema <- client.getSchemaById[StubbedParsedSchema](id)
      } yield schema

      assert {
        test.unsafeRunSync() == schema
      }
    }
  }

  case class StubbedParsedSchema(override val name: String) extends ParsedSchema {
    override def schemaType(): String = "stub"
    override def canonicalString(): String = name
    override def references(): util.List[SchemaReference] =
      new util.ArrayList[SchemaReference]()
    override def isBackwardCompatible(previousSchema: ParsedSchema): util.List[String] =
      new util.ArrayList[String]()
    override def rawSchema(): AnyRef = null
  }
}

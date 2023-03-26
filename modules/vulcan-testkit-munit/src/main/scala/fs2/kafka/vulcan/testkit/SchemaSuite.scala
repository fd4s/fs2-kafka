/*
 * Copyright 2018-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan.testkit

import munit.FunSuite
import vulcan.Codec
import org.apache.avro.SchemaCompatibility
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.kafka.schemaregistry.client.{SchemaRegistryClient, SchemaRegistryClientSettings}
import org.apache.avro.Schema

trait CompatibilityChecker[F[_]] {
  def checkReaderCompatibility[A](
    reader: Codec[A],
    writerSubject: String
  ): F[SchemaCompatibility.SchemaPairCompatibility]

  def checkWriterCompatibility[A](
    writer: Codec[A],
    readerSubject: String
  ): F[SchemaCompatibility.SchemaPairCompatibility]
}

trait SchemaSuite extends FunSuite {

  private def codecAsSchema[A](codec: Codec[A]) = codec.schema.fold(e => fail(e.message), ok => ok)

  def compatibilityChecker(
    clientSettings: SchemaRegistryClientSettings,
    name: String = "schema-compatibility-checker"
  ): Fixture[CompatibilityChecker[IO]] = new Fixture[CompatibilityChecker[IO]](name) {

    private var checker: CompatibilityChecker[IO] = null

    override def apply(): CompatibilityChecker[IO] = checker

    override def beforeAll(): Unit =
      checker = SchemaRegistryClient[IO](clientSettings)
        .map { client =>
          new CompatibilityChecker[IO] {

            private def registrySchema(subject: String): IO[Schema] =
              for {
                metadata <- client.getLatestSchemaMetadata(subject)
                schema <- client.getSchemaById[AvroSchema](metadata.getId)
              } yield schema.rawSchema()

            def checkReaderCompatibility[A](
              reader: Codec[A],
              writerSubject: String
            ): IO[SchemaCompatibility.SchemaPairCompatibility] = {
              val vulcanSchema = codecAsSchema(reader)
              registrySchema(writerSubject).map { regSchema =>
                SchemaCompatibility.checkReaderWriterCompatibility(
                  vulcanSchema,
                  regSchema
                )
              }
            }

            def checkWriterCompatibility[A](writer: Codec[A], readerSubject: String)
              : IO[SchemaCompatibility.SchemaPairCompatibility] = {
              val vulcanSchema = codecAsSchema(writer)
              registrySchema(readerSubject).map { regSchema =>
                SchemaCompatibility.checkReaderWriterCompatibility(
                  regSchema,
                  vulcanSchema
                )
              }
            }

          }
        }
        .unsafeRunSync()
  }
}

/*
 * Copyright 2018-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.kafka.Headers
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import org.scalatest.funspec.AnyFunSpec
import vulcan.Codec

final class AvroSerializerSpec extends AnyFunSpec {
  describe("AvroSerializer") {
    it("can create a serializer") {
      val serializer =
        AvroSerializer[Int].using(avroSettings)

      assert(serializer.forKey.attempt.unsafeRunSync().isRight)
      assert(serializer.forValue.attempt.unsafeRunSync().isRight)
    }

    it("auto-registers union schemas") {
      (avroSerializer[Either[Int, Boolean]]
        .using(avroSettings)
        .forValue
        .flatMap(
          _.serialize(
            "test-union-topic",
            Headers.empty,
            Right(true)
          )
        ))
        .unsafeRunSync()
      assert(
        schemaRegistryClient
          .getLatestSchemaMetadata("test-union-topic-value")
          .getSchema === """["int","boolean"]"""
      )
    }

    it("raises schema errors") {
      val codec: Codec[BigDecimal] =
        Codec.decimal(-1, -1)

      val serializer =
        avroSerializer(codec).using(avroSettings)

      assert(serializer.forKey.attempt.unsafeRunSync().isRight)
      assert(serializer.forValue.attempt.unsafeRunSync().isRight)
    }

    it("toString") {
      assert {
        avroSerializer[Int].toString() startsWith "AvroSerializer$"
      }
    }
  }

  val schemaRegistryClient: MockSchemaRegistryClient =
    new MockSchemaRegistryClient()

  val schemaRegistryClientSettings: SchemaRegistryClientSettings[IO] =
    SchemaRegistryClientSettings[IO]("baseUrl")
      .withAuth(Auth.Basic("username", "password"))
      .withMaxCacheSize(100)
      .withCreateSchemaRegistryClient { (_, _, _) =>
        IO.pure(schemaRegistryClient)
      }

  val avroSettings: AvroSettings[IO] =
    AvroSettings(schemaRegistryClientSettings)
}

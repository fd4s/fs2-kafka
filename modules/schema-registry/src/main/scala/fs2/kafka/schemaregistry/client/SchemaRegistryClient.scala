package fs2.kafka.schemaregistry.client

import cats.effect.kernel.Sync
import io.confluent.kafka.schemaregistry.ParsedSchema
import io.confluent.kafka.schemaregistry.client.SchemaMetadata

import scala.jdk.CollectionConverters.MapHasAsJava

trait SchemaRegistryClient[F[_]] {

  /**
    * Register a schema for a given `Codec` for some type `A`,
    * or return the existing schema id if it already exists.
    * @param subject The subject name
    * @return The schema id
    */
  def register[A](subject: String, schema: ParsedSchema): F[Int]

  /**
    * Get latest schema for the specified subject
    * @param subject The subject name
    * @return Latest available schema for the subject
    */
  def getLatestSchemaMetadata(subject: String): F[SchemaMetadata]

  /**
    * Get the schema for the specified subject with the specified version
    * @param subject The subject name
    * @param version Schema version
    * @return
    */
  def getSchemaMetadata(subject: String, version: Int): F[SchemaMetadata]

  /**
    * Get the schema for the specified `id` and cast it to specified type
    * @param id Schema id
    * @tparam S Schema type
    * @return Schema for the specified `id`
    */
  def getSchemaById[S <: ParsedSchema](id: Int): F[S]

  /**
    * Get the wrapped java instance
    * @return The wrapped Java instance of `SchemaRegistryClient`
    */
  def javaClient: JSchemaRegistryClient
}
object SchemaRegistryClient {

  import cats.implicits._

  private[this] final case class SchemaRegistryClientSettingsImpl[F[_]](
    override val javaClient: JSchemaRegistryClient
  )(implicit F: Sync[F])
      extends SchemaRegistryClient[F] {

    override def register[A](subject: String, schema: ParsedSchema): F[Int] =
      F.delay(javaClient.register(subject, schema))

    override def getLatestSchemaMetadata(subject: String): F[SchemaMetadata] =
      F.delay(javaClient.getLatestSchemaMetadata(subject))

    override def getSchemaMetadata(subject: String, version: Int): F[SchemaMetadata] =
      F.delay(javaClient.getSchemaMetadata(subject, version))

    override def getSchemaById[S <: ParsedSchema](id: Int): F[S] =
      F.delay(javaClient.getSchemaById(id).asInstanceOf[S])
  }

  def apply[F[_]: Sync](baseUrl: String): F[SchemaRegistryClient[F]] =
    SchemaRegistryClient(SchemaRegistryClientSettings(baseUrl))

  def apply[F[_]: Sync](settings: SchemaRegistryClientSettings): F[SchemaRegistryClient[F]] =
    Sync[F]
      .delay(
        new JCachedSchemaRegistryClient(
          settings.baseUrl,
          settings.maxCacheSize,
          settings.properties.asJava
        )
      )
      .map(fromJava(_))

  def fromJava[F[_]: Sync](jclient: JSchemaRegistryClient): SchemaRegistryClient[F] =
    new SchemaRegistryClientSettingsImpl[F](jclient)
}

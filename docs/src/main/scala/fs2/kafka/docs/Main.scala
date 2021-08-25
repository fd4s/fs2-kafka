package fs2.kafka.docs

import fs2.kafka.build.info._
import java.nio.file.{FileSystems, Path}
import scala.collection.Seq

object Main {
  def sourceDirectoryPath(rest: String*): Path =
    FileSystems.getDefault.getPath(sourceDirectory.getAbsolutePath, rest: _*)

  def minorVersion(version: String): String = {
    val Array(major, minor, _) = version.split('.')
    s"$major.$minor"
  }

  def majorVersion(version: String): String = {
    val Array(major, _, _) = version.split('.')
    major
  }

  def minorVersionsString(versions: Seq[String]): String = {
    val minorVersions = versions.map(minorVersion)
    if (minorVersions.size <= 2) minorVersions.mkString(" and ")
    else minorVersions.init.mkString(", ") ++ " and " ++ minorVersions.last
  }

  def main(args: Array[String]): Unit = {
    val scalaMinorVersion = minorVersion(scalaVersion)
    val kafkaDocsVersion = minorVersion(kafkaVersion).filter(_ != '.')

    val settings = mdoc
      .MainSettings()
      .withSiteVariables {
        Map(
          "ORGANIZATION" -> organization,
          "CORE_MODULE_NAME" -> coreModuleName,
          "CORE_CROSS_SCALA_VERSIONS" -> minorVersionsString(coreCrossScalaVersions),
          "VULCAN_MODULE_NAME" -> vulcanModuleName,
          "VULCAN_CROSS_SCALA_VERSIONS" -> minorVersionsString(vulcanCrossScalaVersions),
          "VULCAN_TESTKIT_MUNIT_MODULE_NAME" -> vulcanTeskkitMunitModuleName,
          "LATEST_VERSION" -> latestVersion,
          "LATEST_SNAPSHOT_VERSION" -> latestSnapshotVersion,
          "LATEST_MAJOR_VERSION" -> majorVersion(latestVersion),
          "DOCS_SCALA_MINOR_VERSION" -> scalaMinorVersion,
          "FS2_VERSION" -> fs2Version,
          "KAFKA_VERSION" -> kafkaVersion,
          "VULCAN_VERSION" -> vulcanVersion,
          "CONFLUENT_VERSION" -> confluentVersion,
          "KAFKA_DOCS_VERSION" -> kafkaDocsVersion,
          "SCALA_PUBLISH_VERSIONS" -> minorVersionsString(crossScalaVersions),
          "API_BASE_URL" -> s"/fs2-kafka/api/fs2/kafka",
          "KAFKA_API_BASE_URL" -> s"https://kafka.apache.org/$kafkaDocsVersion/javadoc"
        )
      }
      .withScalacOptions(scalacOptions.mkString(" "))
      .withIn(sourceDirectoryPath("main", "mdoc"))
      .withArgs(args.toList)

    val exitCode = mdoc.Main.process(settings)
    if (exitCode != 0) sys.exit(exitCode)
  }
}

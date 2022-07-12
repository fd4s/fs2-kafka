import laika.ast.Path.Root
import laika.ast.{Image, Length, LengthUnit, SVGSymbolIcon, Target}
import laika.helium.Helium
import laika.helium.config.{
  Favicon,
  FontSizes,
  HeliumIcon,
  IconLink,
  ImageLink,
  ReleaseInfo,
  TextLink
}
import laika.rewrite.link.{ApiLinks, LinkConfig}

ThisBuild / tlBaseVersion := "2.4"

ThisBuild / tlSitePublishBranch := Some("series/2.x")

ThisBuild / tlSiteApiUrl := Some(new URL("https://github.com/fd4s/fs2-kafka/"))

val catsEffectVersion = "3.3.11"

val confluentVersion = "6.2.2"

val fs2Version = "3.2.5"

val kafkaVersion = "2.8.1"

val testcontainersScalaVersion = "0.40.7"

val vulcanVersion = "1.8.3"

val munitVersion = "0.7.29"

val scala212 = "2.12.15"

val scala213 = "2.13.8"

val scala3 = "3.1.2"

lazy val `fs2-kafka` = tlCrossRootProject.aggregate(core, vulcan, `vulcan-testkit-munit`)
//
//  project
//  .in(file("."))
//  .settings(
//    mimaSettings,
//    scalaSettings,
//    noPublishSettings,
//    console := (core / Compile / console).value,
//    Test / console := (core / Test / console).value
//  )
//  .aggregate(core, vulcan, `vulcan-testkit-munit`)

lazy val core = project
  .in(file("modules/core"))
  .settings(
    moduleName := "fs2-kafka",
    name := moduleName.value,
    dependencySettings ++ Seq(
      libraryDependencies ++= Seq(
        "co.fs2" %% "fs2-core" % fs2Version,
        "org.typelevel" %% "cats-effect" % catsEffectVersion,
        "org.apache.kafka" % "kafka-clients" % kafkaVersion
      )
    ),
    publishSettings,
    mimaSettings,
    testSettings
  )

lazy val vulcan = project
  .in(file("modules/vulcan"))
  .settings(
    moduleName := "fs2-kafka-vulcan",
    name := moduleName.value,
    dependencySettings ++ Seq(
      libraryDependencies ++= Seq(
        "com.github.fd4s" %% "vulcan" % vulcanVersion,
        "io.confluent" % "kafka-avro-serializer" % confluentVersion
      )
    ),
    publishSettings,
    mimaSettings,
    testSettings
  )
  .dependsOn(core)

lazy val `vulcan-testkit-munit` = project
  .in(file("modules/vulcan-testkit-munit"))
  .settings(
    moduleName := "fs2-kafka-vulcan-testkit-munit",
    name := moduleName.value,
    dependencySettings ++ Seq(
      libraryDependencies ++= Seq(
        "org.scalameta" %% "munit" % munitVersion
      )
    ),
    publishSettings,
    mimaSettings,
    testSettings
  )
  .dependsOn(vulcan)

def minorVersionsString(versions: Seq[String]): String = {
  val minorVersions = versions.map(minorVersion)
  if (minorVersions.size <= 2) minorVersions.mkString(" and ")
  else minorVersions.init.mkString(", ") ++ " and " ++ minorVersions.last
}

lazy val docs = project
  .in(file("site"))
  .settings(
    tlSiteRelatedProjects := Seq(
      "vulcan" -> new URL("https://github.com/fd4s/vulcan"),
      TypelevelProject.Fs2
    ),
//    tlSiteHeliumConfig := tlSiteHeliumConfig.value.site
//      .favIcons(Favicon.internal(Root / "img" / "favicon.png", "32x32"))
//      .site
//      .topNavigationBar(
//        homeLink = TextLink.internal(
//          path = Root / "overview.md",
//          text = "FS2 Kafka"
//        ), navLinks = Seq()
//      ),
    mdocVariables ++= Map(
      "ORGANIZATION" -> "org.fd4s",
      "API_BASE_URL" -> "/fs2-kafka/api/fs2/kafka",
      "KAFKA_API_BASE_URL" -> s"https://kafka.apache.org/FOOBAR/javadoc",
      "KAFKA_VERSION" -> kafkaVersion,
      "VULCAN_VERSION" -> vulcanVersion,
      "CONFLUENT_VERSION" -> confluentVersion,
      "FS2_VERSION" -> fs2Version,
      "CORE_CROSS_SCALA_VERSIONS" ->
        minorVersionsString(List(scala212, scala213, scala3)),
      "VULCAN_CROSS_SCALA_VERSIONS" ->
        minorVersionsString(List(scala212, scala213, scala3)),
      "CORE_MODULE_NAME" -> "fs2-kafka",
      "VULCAN_MODULE_NAME" -> "fs2-kafka-vulcan",
      "DOCS_SCALA_MINOR_VERSION" -> scala213
    ),
    laikaConfig := laikaConfig.value
      .withConfigValue(
        LinkConfig(
          apiLinks = Seq(
            ApiLinks(
              baseUri = "https://fd4s.github.io/fs2-kafka/api/",
              packagePrefix = "fs2.kafka"
            ),
            ApiLinks(
              baseUri =
                s"https://kafka.apache.org/${minorVersion(kafkaVersion).filter(_ != '.')}/javadoc/",
              packagePrefix = "org.apache.kafka"
            )
          )
        )
      )
      .withConfigValue("VULCAN_MODULE_NAME", "fs2-kafka-vulcan")
      .withConfigValue("CONFLUENT_VERSION", confluentVersion)
      .withConfigValue("VULCAN_VERSION", vulcanVersion)
      .withConfigValue("DOCS_SCALA_MINOR_VERSION", scala213)
      .withConfigValue(
        "CORE_CROSS_SCALA_VERSIONS",
        minorVersionsString(List(scala212, scala213, scala3))
      )

//    moduleName := "fs2-kafka-docs",
//    name := moduleName.value,
//    dependencySettings,
//    scalaSettings,
//    buildInfoSettings
  )
  .dependsOn(core, vulcan, `vulcan-testkit-munit`)
  .enablePlugins(TypelevelSitePlugin)

lazy val dependencySettings = Seq(
  resolvers += "confluent" at "https://packages.confluent.io/maven/",
  libraryDependencies ++= Seq(
    "com.dimafeng" %% "testcontainers-scala-scalatest" % testcontainersScalaVersion,
    "com.dimafeng" %% "testcontainers-scala-kafka" % testcontainersScalaVersion,
    "org.typelevel" %% "discipline-scalatest" % "2.1.5",
    "org.typelevel" %% "cats-effect-laws" % catsEffectVersion,
    "org.typelevel" %% "cats-effect-testkit" % catsEffectVersion,
    "ch.qos.logback" % "logback-classic" % "1.2.11"
  ).map(_ % Test),
  libraryDependencies ++= {
    if (scalaVersion.value.startsWith("3")) Nil
    else
      Seq(
        compilerPlugin(
          ("org.typelevel" %% "kind-projector" % "0.13.2")
            .cross(CrossVersion.full)
        )
      )
  },
  pomPostProcess := { (node: xml.Node) =>
    new xml.transform.RuleTransformer(new xml.transform.RewriteRule {
      def scopedDependency(e: xml.Elem): Boolean =
        e.label == "dependency" && e.child.exists(_.label == "scope")

      override def transform(node: xml.Node): xml.NodeSeq =
        node match {
          case e: xml.Elem if scopedDependency(e) => Nil
          case _                                  => Seq(node)
        }
    }).transform(node).head
  }
)

lazy val buildInfoSettings = Seq(
  buildInfoPackage := "fs2.kafka.build",
  buildInfoObject := "info",
  buildInfoKeys := Seq[BuildInfoKey](
    scalaVersion,
    scalacOptions,
    sourceDirectory,
    BuildInfoKey.map(ThisBuild / version) {
      case (_, v) => "latestSnapshotVersion" -> v
    },
    BuildInfoKey.map(core / moduleName) {
      case (k, v) => "core" ++ k.capitalize -> v
    },
    BuildInfoKey.map(core / crossScalaVersions) {
      case (k, v) => "core" ++ k.capitalize -> v
    },
    BuildInfoKey.map(vulcan / moduleName) {
      case (k, v) => "vulcan" ++ k.capitalize -> v
    },
    BuildInfoKey.map(vulcan / crossScalaVersions) {
      case (k, v) => "vulcan" ++ k.capitalize -> v
    },
    BuildInfoKey.map(`vulcan-testkit-munit` / moduleName) {
      case (k, v) => "vulcanTestkitMunit" ++ k.capitalize -> v
    },
    LocalRootProject / organization,
    core / crossScalaVersions,
    BuildInfoKey("fs2Version" -> fs2Version),
    BuildInfoKey("kafkaVersion" -> kafkaVersion),
    BuildInfoKey("vulcanVersion" -> vulcanVersion),
    BuildInfoKey("confluentVersion" -> confluentVersion)
  )
)

lazy val metadataSettings = Seq(
  organization := "com.github.fd4s"
)

ThisBuild / githubWorkflowTargetBranches := Seq("series/*")

ThisBuild / githubWorkflowBuild := Seq(
  WorkflowStep.Sbt(List("ci")),
  WorkflowStep.Sbt(List("docs/run"), cond = Some(s"matrix.scala == '$scala213'"))
)

ThisBuild / githubWorkflowArtifactUpload := false

//ThisBuild / githubWorkflowJavaVersions := Seq(JavaSpec.temurin("8"), JavaSpec.temurin("17"))
//
//ThisBuild / githubWorkflowPublishTargetBranches :=
//  Seq(RefPredicate.StartsWith(Ref.Tag("v")))

//ThisBuild / githubWorkflowPublish := Seq(
//  WorkflowStep.Sbt(
//    List("ci-release", "docs/docusaurusPublishGhpages"),
//    env = Map(
//      "GIT_DEPLOY_KEY" -> "${{ secrets.GIT_DEPLOY_KEY }}",
//      "PGP_PASSPHRASE" -> "${{ secrets.PGP_PASSPHRASE }}",
//      "PGP_SECRET" -> "${{ secrets.PGP_SECRET }}",
//      "SONATYPE_PASSWORD" -> "${{ secrets.SONATYPE_PASSWORD }}",
//      "SONATYPE_USERNAME" -> "${{ secrets.SONATYPE_USERNAME }}"
//    )
//  )
//)

lazy val publishSettings =
  metadataSettings ++ Seq(
    Test / publishArtifact := false,
    pomIncludeRepository := (_ => false),
    homepage := Some(url("https://fd4s.github.io/fs2-kafka")),
    licenses := List("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt")),
    startYear := Some(2018),
    headerLicense := Some(
      de.heikoseeberger.sbtheader.License.ALv2(
        s"${startYear.value.get}-${java.time.Year.now}",
        "OVO Energy Limited",
        HeaderLicenseStyle.SpdxSyntax
      )
    ),
    headerSources / excludeFilter := HiddenFileFilter,
    developers := List(
      Developer(
        id = "vlovgr",
        name = "Viktor Lövgren",
        email = "github@vlovgr.se",
        url = url("https://vlovgr.se")
      ),
      Developer(
        id = "bplommer",
        name = "Ben Plommer",
        email = "@bplommer", // actually a twitter handle but whatever ¯\_(ツ)_/¯
        url = url("https://github.com/bplommer")
      ),
      Developer(
        id = "LMNet",
        name = "Yuriy Badalyantc",
        email = "lmnet89@gmail.com",
        url = url("https://github.com/LMnet")
      )
    )
  )

lazy val mimaSettings = Seq(
  mimaBinaryIssueFilters ++= {
    import com.typesafe.tools.mima.core._
    // format: off
    Seq(
      ProblemFilters.exclude[Problem]("fs2.kafka.internal.*"),
      ProblemFilters.exclude[IncompatibleSignatureProblem]("*"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("fs2.kafka.vulcan.AvroSettings.registerSchema"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("fs2.kafka.vulcan.AvroSettings.withRegisterSchema"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("fs2.kafka.vulcan.AvroSettings#AvroSettingsImpl.copy"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("fs2.kafka.vulcan.AvroSettings#AvroSettingsImpl.this"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("fs2.kafka.vulcan.AvroSettings#AvroSettingsImpl.apply"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("fs2.kafka.KafkaAdminClient.deleteConsumerGroups"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("fs2.kafka.KafkaProducerConnection.produce"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("fs2.kafka.KafkaProducerConnection.metrics"),
      ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("fs2.kafka.KafkaConsumer.committed"),

      // package-private
      ProblemFilters.exclude[DirectMissingMethodProblem]("fs2.kafka.KafkaProducer.from"),

      // sealed
      ProblemFilters.exclude[ReversedMissingMethodProblem]("fs2.kafka.ConsumerSettings.withDeserializers"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("fs2.kafka.ProducerSettings.withSerializers"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("fs2.kafka.vulcan.AvroSettings.*"),
      ProblemFilters.exclude[FinalMethodProblem]("fs2.kafka.vulcan.AvroSettings.*"),

      // private
        ProblemFilters.exclude[Problem]("fs2.kafka.vulcan.AvroSettings#AvroSettingsImpl.*")
    )
    // format: on
  }
)

ThisBuild / scalaVersion := scala213
ThisBuild / crossScalaVersions := Seq(scala212, scala213, scala3)

lazy val testSettings = Seq(
  Test / logBuffered := false,
  Test / parallelExecution := false,
  Test / testOptions += Tests.Argument("-oDF")
)

def minorVersion(version: String): String = {
  val (major, minor) =
    CrossVersion.partialVersion(version).get
  s"$major.$minor"
}

val updateSiteVariables = taskKey[Unit]("Update site variables")
ThisBuild / updateSiteVariables := {
  val file =
    (LocalRootProject / baseDirectory).value / "website" / "variables.js"

  val variables =
    Map[String, String](
      "organization" -> (LocalRootProject / organization).value,
      "coreModuleName" -> (core / moduleName).value,
      "scalaPublishVersions" -> {
        val minorVersions = (core / crossScalaVersions).value.map(minorVersion)
        if (minorVersions.size <= 2) minorVersions.mkString(" and ")
        else minorVersions.init.mkString(", ") ++ " and " ++ minorVersions.last
      }
    )

  val fileHeader =
    "// Generated by sbt. Do not edit directly."

  val fileContents =
    variables.toList
      .sortBy { case (key, _) => key }
      .map { case (key, value) => s"  $key: '$value'" }
      .mkString(s"$fileHeader\nmodule.exports = {\n", ",\n", "\n};\n")

  IO.write(file, fileContents)
}

def addCommandsAlias(name: String, values: List[String]) =
  addCommandAlias(name, values.mkString(";", ";", ""))

addCommandsAlias(
  "validate",
  List(
    "+clean",
    "+test",
    "+mimaReportBinaryIssues",
    "+scalafmtCheck",
    "scalafmtSbtCheck",
    "+headerCheck",
    "+doc",
    "docs/run"
  )
)

addCommandsAlias(
  "ci",
  List(
    "clean",
    "test",
    "mimaReportBinaryIssues",
    "scalafmtCheck",
    "scalafmtSbtCheck",
    "headerCheck",
    "doc"
  )
)

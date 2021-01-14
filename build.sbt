val catsEffectVersion = "2.3.1"

val catsVersion = "2.3.1"

val confluentVersion = "6.0.1"

val embeddedKafkaVersion = "2.6.0"

val fs2Version = "2.5.0"

val kafkaVersion = "2.6.0"

val vulcanVersion = "1.3.0"

val scala212 = "2.12.10"

val scala213 = "2.13.3"

val scala3 = "3.0.0-M3"

lazy val `fs2-kafka` = project
  .in(file("."))
  .settings(
    mimaSettings,
    scalaSettings,
    noPublishSettings,
    console := (console in (core, Compile)).value,
    console in Test := (console in (core, Test)).value
  )
  .aggregate(core, vulcan)

lazy val core = project
  .in(file("modules/core"))
  .settings(
    moduleName := "fs2-kafka",
    name := moduleName.value,
    dependencySettings ++ Seq(
      libraryDependencies ++= Seq(
        "co.fs2" %% "fs2-core" % fs2Version,
        "org.apache.kafka" % "kafka-clients" % kafkaVersion
      )
    ),
    publishSettings,
    mimaSettings,
    scalaSettings,
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
    scalaSettings,
    testSettings
  )
  .dependsOn(core)

lazy val docs = project
  .in(file("docs"))
  .settings(
    moduleName := "fs2-kafka-docs",
    name := moduleName.value,
    dependencySettings,
    noPublishSettings,
    scalaSettings,
    mdocSettings,
    buildInfoSettings
  )
  .dependsOn(core, vulcan)
  .enablePlugins(BuildInfoPlugin, DocusaurusPlugin, MdocPlugin, ScalaUnidocPlugin)

lazy val dependencySettings = Seq(
  resolvers += "confluent" at "https://packages.confluent.io/maven/",
  libraryDependencies ++= Seq(
    ("io.github.embeddedkafka" %% "embedded-kafka" % embeddedKafkaVersion)
      .withDottyCompat(scalaVersion.value),
    "org.typelevel" %% "discipline-scalatest" % "2.1.1",
    "org.typelevel" %% "cats-effect-laws" % catsEffectVersion,
    "org.typelevel" %% "cats-laws" % catsVersion,
    "org.typelevel" %% "cats-kernel-laws" % catsVersion,
    "ch.qos.logback" % "logback-classic" % "1.2.3",
    "jline" % "jline" % "2.14.2"
  ).map(_ % Test),
  libraryDependencies ++= (if (isDotty.value) Nil
                           else
                             Seq(
                               compilerPlugin(
                                 ("org.typelevel" %% "kind-projector" % "0.11.1")
                                   .cross(CrossVersion.full)
                               )
                             )),
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

lazy val mdocSettings = Seq(
  mdoc := run.in(Compile).evaluated,
  scalacOptions --= Seq("-Xfatal-warnings", "-Ywarn-unused"),
  crossScalaVersions := Seq(scalaVersion.value),
  unidocProjectFilter in (ScalaUnidoc, unidoc) := inProjects(core, vulcan),
  target in (ScalaUnidoc, unidoc) := (baseDirectory in LocalRootProject).value / "website" / "static" / "api",
  cleanFiles += (target in (ScalaUnidoc, unidoc)).value,
  docusaurusCreateSite := docusaurusCreateSite
    .dependsOn(unidoc in Compile)
    .dependsOn(updateSiteVariables in ThisBuild)
    .value,
  docusaurusPublishGhpages :=
    docusaurusPublishGhpages
      .dependsOn(unidoc in Compile)
      .dependsOn(updateSiteVariables in ThisBuild)
      .value,
  // format: off
  scalacOptions in (ScalaUnidoc, unidoc) ++= Seq(
    "-doc-source-url", s"https://github.com/fd4s/fs2-kafka/tree/v${(latestVersion in ThisBuild).value}€{FILE_PATH}.scala",
    "-sourcepath", baseDirectory.in(LocalRootProject).value.getAbsolutePath,
    "-doc-title", "FS2 Kafka",
    "-doc-version", s"v${(latestVersion in ThisBuild).value}"
  )
  // format: on
)

lazy val buildInfoSettings = Seq(
  buildInfoPackage := "fs2.kafka.build",
  buildInfoObject := "info",
  buildInfoKeys := Seq[BuildInfoKey](
    scalaVersion,
    scalacOptions,
    sourceDirectory,
    latestVersion in ThisBuild,
    BuildInfoKey.map(version in ThisBuild) {
      case (_, v) => "latestSnapshotVersion" -> v
    },
    BuildInfoKey.map(moduleName in core) {
      case (k, v) => "core" ++ k.capitalize -> v
    },
    BuildInfoKey.map(crossScalaVersions in core) {
      case (k, v) => "core" ++ k.capitalize -> v
    },
    BuildInfoKey.map(moduleName in vulcan) {
      case (k, v) => "vulcan" ++ k.capitalize -> v
    },
    BuildInfoKey.map(crossScalaVersions in vulcan) {
      case (k, v) => "vulcan" ++ k.capitalize -> v
    },
    organization in LocalRootProject,
    crossScalaVersions in core,
    BuildInfoKey("fs2Version" -> fs2Version),
    BuildInfoKey("kafkaVersion" -> kafkaVersion),
    BuildInfoKey("vulcanVersion" -> vulcanVersion),
    BuildInfoKey("confluentVersion" -> confluentVersion)
  )
)

lazy val metadataSettings = Seq(
  organization := "com.github.fd4s"
)

lazy val publishSettings =
  metadataSettings ++ Seq(
    publishArtifact in Test := false,
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
    excludeFilter.in(headerSources) := HiddenFileFilter,
    developers := List(
      Developer(
        id = "vlovgr",
        name = "Viktor Lövgren",
        email = "github@vlovgr.se",
        url = url("https://vlovgr.se")
      )
    )
  )

lazy val mimaSettings = Seq(
  mimaPreviousArtifacts := {
    if (publishArtifact.value && !isDotty.value) {
      Set(organization.value %% moduleName.value % (previousStableVersion in ThisBuild).value.get)
    } else Set()
  },
  mimaBinaryIssueFilters ++= {
    import com.typesafe.tools.mima.core._
    // format: off
    Seq(
      ProblemFilters.exclude[Problem]("fs2.kafka.internal.*"),
      ProblemFilters.exclude[IncompatibleSignatureProblem]("*"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("fs2.kafka.KafkaProducer.resource"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("fs2.kafka.KafkaProducer.produceRecord"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("fs2.kafka.package.*"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("fs2.kafka.ProducerResource.*"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("fs2.kafka.TransactionalProducerResource.*"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("fs2.kafka.ProducerStream.*"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("fs2.kafka.TransactionalProducerStream.*"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("fs2.kafka.KafkaProducer.resource"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("fs2.kafka.TransactionalKafkaProducer.resource"),
      ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("fs2.kafka.KafkaConsumer.partitionsMapStream"),
      ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("fs2.kafka.KafkaConsumer.stopConsuming"),
      ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("fs2.kafka.KafkaConsumer.commitAsync"),
      ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("fs2.kafka.KafkaConsumer.commitSync"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("fs2.kafka.KafkaAdminClient.*")
    )
    // format: on
  }
)

lazy val noPublishSettings =
  publishSettings ++ Seq(
    skip in publish := true,
    publishArtifact := false
  )

lazy val scalaSettings = Seq(
  scalaVersion := scala213,
  crossScalaVersions := Seq(scala212, scala213, scala3),
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding",
    "UTF-8",
    "-feature",
    "-language:implicitConversions",
    "-unchecked"
  ) ++ (
    if (scalaVersion.value.startsWith("2.13"))
      Seq(
        "-language:higherKinds",
        "-Xlint",
        "-Ywarn-dead-code",
        "-Ywarn-numeric-widen",
        "-Ywarn-value-discard",
        "-Ywarn-unused",
        "-Xfatal-warnings"
      )
    else if (scalaVersion.value.startsWith("2.12"))
      Seq(
        "-language:higherKinds",
        "-Xlint",
        "-Yno-adapted-args",
        "-Ywarn-dead-code",
        "-Ywarn-numeric-widen",
        "-Ywarn-value-discard",
        "-Ywarn-unused",
        "-Ypartial-unification",
        "-Xfatal-warnings"
      )
    else
      Seq(
        "-Ykind-projector",
        "-source:3.0-migration",
        "-Xignore-scala2-macros"
      )
  ),
  scalacOptions in (Compile, console) --= Seq("-Xlint", "-Ywarn-unused"),
  scalacOptions in (Test, console) := (scalacOptions in (Compile, console)).value,
  Compile / unmanagedSourceDirectories ++=
    Seq(
      baseDirectory.value / "src" / "main" / (if (scalaVersion.value.startsWith("2.12"))
                                                "scala-2.12"
                                              else "scala-2.13+")
    )
)

lazy val testSettings = Seq(
  logBuffered in Test := false,
  parallelExecution in Test := false,
  testOptions in Test += Tests.Argument("-oDF")
)

def minorVersion(version: String): String = {
  val (major, minor) =
    CrossVersion.partialVersion(version).get
  s"$major.$minor"
}

val latestVersion = settingKey[String]("Latest stable released version")
latestVersion in ThisBuild := {
  val snapshot = (isSnapshot in ThisBuild).value
  val stable = (isVersionStable in ThisBuild).value

  if (!snapshot && stable) {
    (version in ThisBuild).value
  } else {
    (previousStableVersion in ThisBuild).value.get
  }
}

val updateSiteVariables = taskKey[Unit]("Update site variables")
updateSiteVariables in ThisBuild := {
  val file =
    (baseDirectory in LocalRootProject).value / "website" / "variables.js"

  val variables =
    Map[String, String](
      "organization" -> (organization in LocalRootProject).value,
      "coreModuleName" -> (moduleName in core).value,
      "latestVersion" -> (latestVersion in ThisBuild).value,
      "scalaPublishVersions" -> {
        val minorVersions = (crossScalaVersions in core).value.map(minorVersion)
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

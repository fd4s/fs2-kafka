import ReleaseTransformations._

val catsEffectVersion = "2.0.0-M4"

val catsVersion = "2.0.0-M4"

val confluentVersion = "5.2.2"

val fs2Version = "1.1.0-M1"

val kafkaVersion = "2.3.0"

val vulcanVersion = "0.2.0-M3"

val scala212 = "2.12.8"

val scala213 = "2.13.0"

lazy val root = project
  .in(file("."))
  .settings(
    mimaSettings,
    noPublishSettings,
    scalaVersion := scala212,
    console := (console in (core, Compile)).value,
    console in Test := (console in (core, Test)).value
  )
  .aggregate(core, vulcan)

lazy val core = project
  .in(file("modules/core"))
  .settings(
    moduleName := "fs2-kafka",
    name := moduleName.value,
    dependencySettings,
    coverageSettings,
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
        "com.ovoenergy" %% "vulcan" % vulcanVersion,
        "io.confluent" % "kafka-avro-serializer" % confluentVersion
      )
    ),
    coverageSettings,
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
    "co.fs2" %% "fs2-core" % fs2Version,
    "org.typelevel" %% "cats-effect" % catsEffectVersion,
    "org.apache.kafka" % "kafka-clients" % kafkaVersion
  ),
  libraryDependencies ++= Seq(
    "org.typelevel" %% "cats-testkit" % catsVersion,
    "org.typelevel" %% "cats-effect-laws" % catsEffectVersion,
    "ch.qos.logback" % "logback-classic" % "1.2.3"
  ).map(_ % Test),
  libraryDependencies ++= {
    if (!scalaVersion.value.startsWith("2.13"))
      Seq(
        "io.github.embeddedkafka" %% "embedded-kafka" % "2.3.0",
        "org.apache.kafka" %% "kafka" % kafkaVersion
      ).map(_ % Test)
    else Seq.empty
  },
  addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.0"),
  addCompilerPlugin("org.typelevel" % "kind-projector" % "0.10.3" cross CrossVersion.binary)
)

lazy val mdocSettings = Seq(
  mdoc := run.in(Compile).evaluated,
  scalacOptions --= Seq("-Xfatal-warnings", "-Ywarn-unused"),
  crossScalaVersions := Seq(scala212),
  unidocProjectFilter in (ScalaUnidoc, unidoc) := inProjects(core, vulcan),
  target in (ScalaUnidoc, unidoc) := (baseDirectory in LocalRootProject).value / "website" / "static" / "api",
  cleanFiles += (target in (ScalaUnidoc, unidoc)).value,
  docusaurusPublishGhpages := docusaurusPublishGhpages.dependsOn(unidoc in Compile).value,
  // format: off
  scalacOptions in (ScalaUnidoc, unidoc) ++= Seq(
    "-doc-source-url", s"https://github.com/ovotech/fs2-kafka/tree/v${(latestVersion in ThisBuild).value}€{FILE_PATH}.scala",
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
    organization in root,
    crossScalaVersions in core,
    BuildInfoKey("fs2Version" -> fs2Version),
    BuildInfoKey("kafkaVersion" -> kafkaVersion),
    BuildInfoKey("vulcanVersion" -> vulcanVersion),
    BuildInfoKey("confluentVersion" -> confluentVersion)
  )
)

lazy val metadataSettings = Seq(
  organization := "com.ovoenergy",
  organizationName := "OVO Energy Limited",
  organizationHomepage := Some(url("https://ovoenergy.com"))
)

lazy val coverageSettings = Seq(
  coverageExcludedPackages := List(
    "fs2.kafka.internal.OrElse"
  ).mkString(";")
)

lazy val publishSettings =
  metadataSettings ++ Seq(
    publishMavenStyle := true,
    publishArtifact in Test := false,
    publishTo := sonatypePublishTo.value,
    pomIncludeRepository := (_ => false),
    homepage := Some(url("https://ovotech.github.io/fs2-kafka")),
    licenses := List("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt")),
    startYear := Some(2018),
    headerLicense := Some(
      de.heikoseeberger.sbtheader.License.ALv2(
        s"${startYear.value.get}-${java.time.Year.now}",
        organizationName.value
      )
    ),
    excludeFilter.in(headerSources) := HiddenFileFilter || "*OrElse.scala",
    developers := List(
      Developer(
        id = "vlovgr",
        name = "Viktor Lövgren",
        email = "github@vlovgr.se",
        url = url("https://vlovgr.se")
      )
    ),
    scmInfo := Some(
      ScmInfo(
        url("https://github.com/ovotech/fs2-kafka"),
        "scm:git@github.com:ovotech/fs2-kafka.git"
      )
    ),
    releaseCrossBuild := false, // See https://github.com/sbt/sbt-release/issues/214
    releaseUseGlobalVersion := true,
    releaseTagName := s"v${(version in ThisBuild).value}",
    releaseTagComment := s"Release version ${(version in ThisBuild).value}",
    releaseCommitMessage := s"Set version to ${(version in ThisBuild).value}",
    releaseProcess := Seq[ReleaseStep](
      checkSnapshotDependencies,
      inquireVersions,
      runClean,
      releaseStepCommandAndRemaining("+compile"),
      setReleaseVersion,
      setLatestVersion,
      releaseStepTask(updateSiteVariables in ThisBuild),
      releaseStepTask(addDateToReleaseNotes in ThisBuild),
      commitReleaseVersion,
      tagRelease,
      releaseStepCommandAndRemaining("+publish"),
      releaseStepCommand("sonatypeRelease"),
      setNextVersion,
      commitNextVersion,
      pushChanges,
      releaseStepCommand("docs/docusaurusPublishGhpages")
    )
  )

lazy val mimaSettings = Seq(
  mimaFailOnNoPrevious := false,
  mimaPreviousArtifacts := {
    val released = !unreleasedModuleNames.value.contains(moduleName.value)
    val publishing = publishArtifact.value

    if (publishing && released)
      binaryCompatibleVersions.value
        .map(version => organization.value %% moduleName.value % version)
    else
      Set()
  },
  mimaBinaryIssueFilters ++= {
    import com.typesafe.tools.mima.core._
    // format: off
    Seq(
      ProblemFilters.exclude[Problem]("fs2.kafka.internal.*")
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
  scalaVersion := scala212,
  crossScalaVersions := Seq(scala212, scala213),
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding",
    "UTF-8",
    "-feature",
    "-language:existentials",
    "-language:higherKinds",
    "-language:implicitConversions",
    "-unchecked",
    "-Xfatal-warnings",
    "-Xlint",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Ywarn-value-discard",
    "-Xfuture",
    "-Ywarn-unused",
    "-Ypartial-unification"
  ).filter {
    case ("-Yno-adapted-args" | "-Ypartial-unification" | "-Xfuture")
        if scalaVersion.value.startsWith("2.13") =>
      false
    case _ => true
  },
  scalacOptions in (Compile, console) --= Seq("-Xlint", "-Ywarn-unused"),
  scalacOptions in (Test, console) := (scalacOptions in (Compile, console)).value
)

lazy val testSettings = Seq(
  logBuffered in Test := false,
  parallelExecution in Test := false,
  testOptions in Test += Tests.Argument("-oDF"),
  excludeFilter.in(Test, unmanagedSources) := {
    if (scalaVersion.value.startsWith("2.13"))
      HiddenFileFilter ||
      "BaseKafkaSpec.scala" ||
      "HeaderDeserializerSpec.scala" ||
      "KafkaAdminClientSpec.scala" ||
      "KafkaConsumerSpec.scala" ||
      "KafkaProducerSpec.scala" ||
      "TransactionalKafkaProducerSpec.scala"
    else
      (excludeFilter.in(Test, unmanagedSources)).value
  }
)

def minorVersion(version: String): String = {
  val (major, minor) =
    CrossVersion.partialVersion(version).get
  s"$major.$minor"
}

val releaseNotesFile = taskKey[File]("Release notes for current version")
releaseNotesFile in ThisBuild := {
  val currentVersion = (version in ThisBuild).value
  file("notes") / s"$currentVersion.markdown"
}

val updateSiteVariables = taskKey[Unit]("Update site variables")
updateSiteVariables in ThisBuild := {
  val file = (baseDirectory in root).value / "website" / "siteConfig.js"
  val lines = IO.read(file).trim.split('\n').toVector

  val variables =
    Map[String, String](
      "organization" -> (organization in root).value,
      "coreModuleName" -> (moduleName in core).value,
      "vulcanModuleName" -> (moduleName in vulcan).value,
      "latestVersion" -> (latestVersion in ThisBuild).value,
      "scalaMinorVersion" -> minorVersion((scalaVersion in core).value),
      "scalaPublishVersions" -> {
        val minorVersions = (crossScalaVersions in core).value.map(minorVersion)
        if (minorVersions.size <= 2) minorVersions.mkString(" and ")
        else minorVersions.init.mkString(", ") ++ " and " ++ minorVersions.last
      }
    )

  val newLine =
    variables.toList
      .map { case (k, v) => s"$k: '$v'" }
      .mkString("const buildInfo = { ", ", ", " };")

  val lineIndex = lines.indexWhere(_.trim.startsWith("const buildInfo"))
  val newLines = lines.updated(lineIndex, newLine)
  val newFileContents = newLines.mkString("", "\n", "\n")
  IO.write(file, newFileContents)

  sbtrelease.Vcs.detect((baseDirectory in root).value).foreach { vcs =>
    vcs.add(file.getAbsolutePath).!
    vcs
      .commit(
        s"Update site variables for v${(version in ThisBuild).value}",
        sign = true,
        signOff = false
      )
      .!
  }
}

val ensureReleaseNotesExists = taskKey[Unit]("Ensure release notes exists")
ensureReleaseNotesExists in ThisBuild := {
  val currentVersion = (version in ThisBuild).value
  val notes = releaseNotesFile.value
  if (!notes.isFile) {
    throw new IllegalStateException(
      s"no release notes found for version [$currentVersion] at [$notes]."
    )
  }
}

val addDateToReleaseNotes = taskKey[Unit]("Add current date to release notes")
addDateToReleaseNotes in ThisBuild := {
  ensureReleaseNotesExists.value

  val dateString = {
    val formatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val now = java.time.ZonedDateTime.now()
    now.format(formatter)
  }

  val file = releaseNotesFile.value
  val newContents = IO.read(file).trim + s"\n\nReleased on $dateString.\n"
  IO.write(file, newContents)

  sbtrelease.Vcs.detect((baseDirectory in root).value).foreach { vcs =>
    vcs.add(file.getAbsolutePath).!
    vcs
      .commit(
        s"Add release date for v${(version in ThisBuild).value}",
        sign = true,
        signOff = false
      )
      .!
  }
}

def addCommandsAlias(name: String, values: List[String]) =
  addCommandAlias(name, values.mkString(";", ";", ""))

addCommandsAlias(
  "validate",
  List(
    "+clean",
    "+coverage",
    "+test",
    "+coverageReport",
    "+mimaReportBinaryIssues",
    "scalafmtCheck",
    "scalafmtSbtCheck",
    "headerCheck",
    "+doc",
    "docs/run"
  )
)

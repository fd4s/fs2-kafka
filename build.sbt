import ReleaseTransformations._

val catsVersion = "2.0.0-M4"

val catsEffectVersion = "2.0.0-M4"

val fs2Version = "1.1.0-M1"

val kafkaVersion = "2.2.1"

lazy val root = project
  .in(file("."))
  .settings(
    dependencySettings,
    coverageSettings,
    noPublishSettings,
    mimaSettings,
    scalaSettings,
    testSettings,
    console := (console in (core, Compile)).value,
    console in Test := (console in (core, Test)).value
  )
  .aggregate(core)

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

lazy val docs = project
  .in(file("docs"))
  .settings(
    moduleName := "fs2-kafka-docs",
    name := moduleName.value,
    noPublishSettings,
    scalaSettings,
    mdocSettings,
    buildInfoSettings
  )
  .dependsOn(core)
  .enablePlugins(BuildInfoPlugin, DocusaurusPlugin, MdocPlugin)

lazy val dependencySettings = Seq(
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
        "io.github.embeddedkafka" %% "embedded-kafka" % "2.2.0",
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
  crossScalaVersions := Seq(scalaVersion.value)
)

lazy val buildInfoSettings = Seq(
  buildInfoPackage := "fs2.kafka.build",
  buildInfoObject := "info",
  buildInfoKeys := Seq[BuildInfoKey](
    scalaVersion,
    scalacOptions,
    sourceDirectory,
    latestVersion in ThisBuild,
    moduleName in core,
    organization in root,
    crossScalaVersions in root,
    BuildInfoKey("fs2Version" -> fs2Version),
    BuildInfoKey("kafkaVersion" -> kafkaVersion)
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
    releaseCrossBuild := true,
    releaseUseGlobalVersion := true,
    releaseTagName := s"v${(version in ThisBuild).value}",
    releaseTagComment := s"Release version ${(version in ThisBuild).value}",
    releaseCommitMessage := s"Set version to ${(version in ThisBuild).value}",
    releaseProcess := Seq[ReleaseStep](
      checkSnapshotDependencies,
      inquireVersions,
      runClean,
      runTest,
      setReleaseVersion,
      setLatestVersion,
      releaseStepTask(updateSiteVariables in ThisBuild),
      releaseStepTask(addDateToReleaseNotes in ThisBuild),
      commitReleaseVersion,
      tagRelease,
      publishArtifacts,
      releaseStepCommand("sonatypeRelease"),
      setNextVersion,
      commitNextVersion,
      pushChanges,
      releaseStepCommand("docs/docusaurusPublishGhpages")
    )
  )

lazy val mimaSettings = Seq(
  mimaPreviousArtifacts := {
    if (publishArtifact.value)
      binaryCompatibleVersions.value
        .map(version => organization.value %% moduleName.value % version)
    else
      Set.empty
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
  scalaVersion := "2.12.8",
  crossScalaVersions := Seq("2.11.12", scalaVersion.value, "2.13.0"),
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

val validateDocs = taskKey[Unit]("Validate documentation")
validateDocs in ThisBuild := (run in (docs, Compile)).toTask(" ").value

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
      "moduleName" -> (moduleName in core).value,
      "latestVersion" -> (latestVersion in ThisBuild).value,
      "scalaMinorVersion" -> minorVersion((scalaVersion in root).value),
      "scalaPublishVersions" -> {
        val minorVersions = (crossScalaVersions in root).value.map(minorVersion)
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
    "clean",
    "coverage",
    "test",
    "coverageReport",
    "mimaReportBinaryIssues",
    "scalafmtCheck",
    "scalafmtSbtCheck",
    "headerCheck",
    "doc"
  )
)

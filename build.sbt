import ReleaseTransformations._

val fs2Version = "1.0.2"

val kafkaVersion = "2.0.1"

lazy val `fs2-kafka` = project
  .in(file("."))
  .settings(
    moduleName := "fs2-kafka",
    name := moduleName.value,
    dependencySettings,
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
  .dependsOn(`fs2-kafka`)
  .enablePlugins(BuildInfoPlugin, DocusaurusPlugin)

lazy val dependencySettings = Seq(
  libraryDependencies ++= Seq(
    "co.fs2" %% "fs2-core" % fs2Version,
    "org.apache.kafka" % "kafka-clients" % kafkaVersion
  ),
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "3.0.5",
    "org.scalacheck" %% "scalacheck" % "1.14.0",
    "net.manub" %% "scalatest-embedded-kafka" % "2.0.0"
  ).map(_ % Test)
)

lazy val mdocSettings = Seq(
  mainClass in Compile := Some("fs2.kafka.docs.Main"),
  crossScalaVersions := Seq(scalaVersion.value),
  libraryDependencies += "com.geirsson" %% "mdoc" % "0.7.1"
)

lazy val buildInfoSettings = Seq(
  buildInfoPackage := "fs2.kafka.build",
  buildInfoObject := "info",
  buildInfoKeys := Seq[BuildInfoKey](
    BuildInfoKey.map(organization in `fs2-kafka`) { case (_, v)  => "organization" -> v },
    BuildInfoKey.map(moduleName in `fs2-kafka`) { case (_, v)    => "moduleName" -> v },
    BuildInfoKey.map(latestVersion in ThisBuild) { case (_, v)   => "latestVersion" -> v },
    BuildInfoKey.map(scalacOptions in `fs2-kafka`) { case (_, v) => "scalacOptions" -> v },
    BuildInfoKey
      .map(crossScalaVersions in `fs2-kafka`) { case (_, v) => "crossScalaVersions" -> v },
    BuildInfoKey.map(scalaVersion) { case (_, v)            => "scalaVersionDocs" -> v },
    BuildInfoKey("fs2Version" -> fs2Version),
    BuildInfoKey("kafkaVersion" -> kafkaVersion),
  )
)

lazy val metadataSettings = Seq(
  organization := "com.ovoenergy",
  organizationName := "OVO Energy Ltd",
  organizationHomepage := Some(url("https://ovoenergy.com"))
)

lazy val publishSettings =
  metadataSettings ++ Seq(
    publishMavenStyle := true,
    publishArtifact in Test := false,
    publishTo := sonatypePublishTo.value,
    pomIncludeRepository := (_ => false),
    homepage := Some(url("https://github.com/ovotech/fs2-kafka")),
    licenses := List("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt")),
    startYear := Some(2018),
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
    useGpg := false,
    releaseCrossBuild := true,
    releaseUseGlobalVersion := true,
    releaseTagName := s"v${(version in ThisBuild).value}",
    releasePublishArtifactsAction := PgpKeys.publishSigned.value,
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
    Seq()
    // format: on
  }
)

lazy val noPublishSettings =
  metadataSettings ++ Seq(
    skip in publish := true,
    publishArtifact := false
  )

lazy val scalaSettings = Seq(
  scalaVersion := "2.12.8",
  crossScalaVersions := Seq("2.11.12", scalaVersion.value),
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
  ),
  scalacOptions in (Compile, console) --= Seq("-Xlint", "-Ywarn-unused"),
  scalacOptions in (Test, console) := (scalacOptions in (Compile, console)).value
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

val validateDocs = taskKey[Unit]("Validate documentation")
validateDocs in ThisBuild := (run in (docs, Compile)).toTask(" ").value

val releaseNotesFile = taskKey[File]("Release notes for current version")
releaseNotesFile in ThisBuild := {
  val currentVersion = (version in ThisBuild).value
  file("notes") / s"$currentVersion.markdown"
}

val updateSiteVariables = taskKey[Unit]("Update site variables")
updateSiteVariables in ThisBuild := {
  val file = (baseDirectory in `fs2-kafka`).value / "website" / "siteConfig.js"
  val lines = IO.read(file).trim.split('\n').toVector

  val latestVersionString = (latestVersion in ThisBuild).value.toString
  val latestMinorVersionString = {
    val latestVersionString = (latestVersion in ThisBuild).value.toString
    val (major, minor) = CrossVersion.partialVersion(latestVersionString).get
    s"$major.$minor"
  }
  val organizationString = (organization in `fs2-kafka`).value
  val moduleNameString = (moduleName in `fs2-kafka`).value
  val scalaMinorVersion = minorVersion((scalaVersion in `fs2-kafka`).value)
  val scalaPublishVersions = {
    val minorVersions = (crossScalaVersions in `fs2-kafka`).value.map(minorVersion)
    if (minorVersions.size <= 2) minorVersions.mkString(" and ")
    else minorVersions.init.mkString(", ") ++ " and " ++ minorVersions.last
  }

  val lineIndex = lines.indexWhere(_.trim.startsWith("const buildInfo"))
  val newLine =
    s"const buildInfo = { scalaMinorVersion: '$scalaMinorVersion', organization: '$organizationString', moduleName: '$moduleNameString', latestVersion: '$latestVersionString', latestMinorVersion: '$latestMinorVersionString', scalaPublishVersions: '$scalaPublishVersions' };"
  val newLines = lines.updated(lineIndex, newLine)
  val newFileContents = newLines.mkString("", "\n", "\n")
  IO.write(file, newFileContents)

  sbtrelease.Vcs.detect((baseDirectory in `fs2-kafka`).value).foreach { vcs =>
    vcs.add(file.getAbsolutePath).!
    vcs.commit(s"Update site variables for v${(version in ThisBuild).value}", sign = true).!
  }
}

val ensureReleaseNotesExists = taskKey[Unit]("Ensure release notes exists")
ensureReleaseNotesExists in ThisBuild := {
  val currentVersion = (version in ThisBuild).value
  val notes = releaseNotesFile.value
  if (!notes.isFile) {
    throw new IllegalStateException(
      s"no release notes found for version [$currentVersion] at [$notes].")
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

  sbtrelease.Vcs.detect((baseDirectory in `fs2-kafka`).value).foreach { vcs =>
    vcs.add(file.getAbsolutePath).!
    vcs.commit(s"Add release date for v${(version in ThisBuild).value}", sign = true).!
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

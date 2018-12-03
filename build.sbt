import ReleaseTransformations._

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
    mdocSettings
  )
  .dependsOn(`fs2-kafka`)

lazy val dependencySettings = Seq(
  libraryDependencies ++= Seq(
    "org.typelevel" %% "cats-effect" % "1.1.0-M1",
    "co.fs2" %% "fs2-core" % "1.0.0",
    "org.apache.kafka" % "kafka-clients" % "2.0.1"
  ),
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "3.0.5",
    "org.scalacheck" %% "scalacheck" % "1.14.0",
    "net.manub" %% "scalatest-embedded-kafka" % "2.0.0"
  ).map(_ % Test)
)

lazy val mdocSettings = Seq(
  scalaVersion := "2.12.7",
  crossScalaVersions := Seq(scalaVersion.value),
  libraryDependencies += "com.geirsson" % "mdoc" % "0.5.3" cross CrossVersion.full
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
      releaseStepTask(updateReadme in ThisBuild),
      releaseStepTask(addDateToReleaseNotes in ThisBuild),
      commitReleaseVersion,
      tagRelease,
      publishArtifacts,
      releaseStepCommand("sonatypeRelease"),
      setNextVersion,
      commitNextVersion,
      pushChanges
    )
  )

lazy val mimaSettings = Seq(
  mimaPreviousArtifacts := {
    def isPublishing = publishArtifact.value

    latestBinaryCompatibleVersion.value match {
      case Some(version) if isPublishing =>
        Set(organization.value %% moduleName.value % version)
      case _ =>
        Set.empty
    }
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
  scalaVersion := "2.12.7",
  crossScalaVersions := Seq(scalaVersion.value, "2.11.12"),
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

def runMdoc(args: String*) = Def.taskDyn {
  val in = (baseDirectory in `fs2-kafka`).value / "docs"
  val out = (baseDirectory in `fs2-kafka`).value
  val scalacOptionsString = {
    val scalacOptionsValue = (scalacOptions in Compile).value
    val excludedOptions = Seq("-Xfatal-warnings")
    (scalacOptionsValue diff excludedOptions).mkString(" ")
  }
  val argsString = args.mkString(" ")
  val siteVariables = List[(String, String)](
    "LATEST_VERSION" -> (latestVersion in ThisBuild).value.toString,
    "LATEST_MINOR_VERSION" -> {
      val latestVersionString = (latestVersion in ThisBuild).value.toString
      val (major, minor) = CrossVersion.partialVersion(latestVersionString).get
      s"$major.$minor." // Add trailing dot to workaround: https://github.com/olafurpg/mdoc/issues/102
    }
  ).map { case (k, v) => s"""--site.$k "$v"""" }.mkString(" ")
  (runMain in (docs, Compile)).toTask {
    s""" mdoc.Main --in "$in" --out "$out" --exclude "target" --scalac-options "$scalacOptionsString" $siteVariables $argsString"""
  }
}

val generateReadme = taskKey[Unit]("Generates the readme using mdoc")
generateReadme in ThisBuild := runMdoc().value

val updateReadme = taskKey[Unit]("Generates and commits the readme")
updateReadme in ThisBuild := {
  (generateReadme in ThisBuild).value
  sbtrelease.Vcs.detect((baseDirectory in `fs2-kafka`).value).foreach { vcs =>
    vcs.add("readme.md").!
    vcs.commit("Update readme to latest version", sign = true).!
  }
}

val releaseNotesFile = taskKey[File]("Release notes for current version")
releaseNotesFile in ThisBuild := {
  val currentVersion = (version in ThisBuild).value
  file("notes") / s"$currentVersion.markdown"
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

addCommandsAlias(
  "validateDocs",
  List(
    "generateReadme"
  )
)

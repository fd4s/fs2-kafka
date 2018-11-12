import ReleaseTransformations._

lazy val `fs2-kafka` = project
  .in(file("."))
  .settings(
    dependencySettings,
    metadataSettings,
    mimaSettings,
    releaseSettings,
    resolverSettings,
    scalaSettings,
    testSettings
  )

lazy val docs = project
  .in(file("docs"))
  .settings(
    metadataSettings,
    noPublishSettings,
    scalaSettings,
    mdocSettings
  )
  .dependsOn(`fs2-kafka`)

lazy val dependencySettings = Seq(
  libraryDependencies ++= Seq(
    "org.typelevel" %% "cats-effect" % "1.1.0-M1",
    "co.fs2" %% "fs2-core" % "1.0.0",
    "org.apache.kafka" % "kafka-clients" % "2.0.0"
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
  bintrayOrganization := Some("ovotech"),
  licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.txt")),
  startYear := Some(2018)
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
    Seq(
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("fs2.kafka.KafkaConsumerActor.this"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("fs2.kafka.KafkaConsumerActor.this"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("fs2.kafka.ConsumerSettings#ConsumerSettingsImpl.apply"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("fs2.kafka.ConsumerSettings.commitRecovery"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("fs2.kafka.ConsumerSettings.withCommitRecovery"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("fs2.kafka.ConsumerSettings#ConsumerSettingsImpl.copy"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("fs2.kafka.ConsumerSettings#ConsumerSettingsImpl.this"),
      ProblemFilters.exclude[MissingClassProblem]("fs2.kafka.KafkaConsumerActor$Request$Shutdown"),
      ProblemFilters.exclude[MissingClassProblem]("fs2.kafka.KafkaConsumerActor$Request$Shutdown$"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("fs2.kafka.KafkaConsumerActor#State.asShutdown"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("fs2.kafka.KafkaConsumerActor#State.running"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("fs2.kafka.KafkaConsumerActor#State.copy"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("fs2.kafka.KafkaConsumerActor#State.copy$default$4"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("fs2.kafka.KafkaConsumerActor#State.this"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("fs2.kafka.KafkaConsumerActor#State.apply"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("fs2.kafka.KafkaConsumerActor#Request#Assignment.apply"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("fs2.kafka.KafkaConsumerActor#Request#Revoked.partitions"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("fs2.kafka.KafkaConsumerActor#Request#Revoked.copy"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("fs2.kafka.KafkaConsumerActor#Request#Revoked.copy$default$1"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("fs2.kafka.KafkaConsumerActor#Request#Revoked.this"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("fs2.kafka.KafkaConsumerActor#ExpiringFetch.complete"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("fs2.kafka.KafkaConsumerActor#Request#Revoked.apply"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("fs2.kafka.KafkaConsumerActor#Request#Assignment.copy"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("fs2.kafka.KafkaConsumerActor#Request#Assignment.this"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("fs2.kafka.KafkaConsumerActor#State.withFetch"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("fs2.kafka.KafkaConsumerActor#State.copy$default$3"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("fs2.kafka.KafkaConsumer.parallelPartitionedStream")
    )
    // format: on
  }
)

lazy val noPublishSettings = Seq(
  skip in publish := true,
  publishArtifact := false
)

lazy val releaseSettings = Seq(
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
    commitReleaseVersion,
    tagRelease,
    publishArtifacts,
    setNextVersion,
    commitNextVersion,
    pushChanges
  )
)

lazy val resolverSettings = Seq(
  resolvers += Resolver.bintrayRepo("ovotech", "maven")
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
  )
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

def addCommandsAlias(name: String, values: List[String]) =
  addCommandAlias(name, values.mkString(";", ";", ""))

addCommandsAlias(
  "validate",
  List(
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

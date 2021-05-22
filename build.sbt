val catsEffectVersion = "3.1.1"

val catsVersion = "2.6.1"

val confluentVersion = "6.1.1"

val fs2Version = "3.0.4"

val kafkaVersion = "2.8.0"

val testcontainersScalaVersion = "0.39.4"

val vulcanVersion = "1.7.1"

val scala212 = "2.12.13"

val scala213 = "2.13.6"

val scala3 = "3.0.0"

lazy val `fs2-kafka` = project
  .in(file("."))
  .settings(
    mimaSettings,
    scalaSettings,
    noPublishSettings,
    console := (core / Compile / console).value,
    Test / console := (core / Test / console).value
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
        "org.typelevel" %% "cats-effect" % catsEffectVersion,
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
    ("com.dimafeng" %% "testcontainers-scala-scalatest" % testcontainersScalaVersion)
      .cross(CrossVersion.for3Use2_13),
    ("com.dimafeng" %% "testcontainers-scala-kafka" % testcontainersScalaVersion)
      .cross(CrossVersion.for3Use2_13),
    "org.typelevel" %% "discipline-scalatest" % "2.1.5",
    "org.typelevel" %% "cats-effect-laws" % catsEffectVersion,
    "org.typelevel" %% "cats-effect-testkit" % catsEffectVersion,
    "ch.qos.logback" % "logback-classic" % "1.2.3"
  ).map(_ % Test),
  libraryDependencies ++= {
    if (scalaVersion.value.startsWith("3")) Nil
    else
      Seq(
        compilerPlugin(
          ("org.typelevel" %% "kind-projector" % "0.13.0")
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

lazy val mdocSettings = Seq(
  mdoc := (Compile / run).evaluated,
  scalacOptions --= Seq("-Xfatal-warnings", "-Ywarn-unused"),
  crossScalaVersions := Seq(scalaVersion.value),
  ScalaUnidoc / unidoc / unidocProjectFilter := inProjects(core, vulcan),
  ScalaUnidoc / unidoc / target := (LocalRootProject / baseDirectory).value / "website" / "static" / "api",
  cleanFiles += (ScalaUnidoc / unidoc / target).value,
  docusaurusCreateSite := docusaurusCreateSite
    .dependsOn(Compile / unidoc)
    .dependsOn(ThisBuild / updateSiteVariables)
    .value,
  docusaurusPublishGhpages :=
    docusaurusPublishGhpages
      .dependsOn(Compile / unidoc)
      .dependsOn(ThisBuild / updateSiteVariables)
      .value,
  // format: off
  ScalaUnidoc / unidoc / scalacOptions ++= Seq(
    "-doc-source-url", s"https://github.com/fd4s/fs2-kafka/tree/v${(ThisBuild / latestVersion).value}€{FILE_PATH}.scala",
    "-sourcepath", (LocalRootProject / baseDirectory).value.getAbsolutePath,
    "-doc-title", "FS2 Kafka",
    "-doc-version", s"v${(ThisBuild / latestVersion).value}"
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
    ThisBuild / latestVersion,
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

ThisBuild / githubWorkflowTargetTags ++= Seq("v*")
ThisBuild / githubWorkflowPublishTargetBranches :=
  Seq(RefPredicate.StartsWith(Ref.Tag("v")))

ThisBuild / githubWorkflowPublish := Seq(
  WorkflowStep.Sbt(
    List("ci-release", "docs/docusaurusPublishGhpages"),
    env = Map(
      "GIT_DEPLOY_KEY" -> "${{ secrets.GIT_DEPLOY_KEY }}",
      "PGP_PASSPHRASE" -> "${{ secrets.PGP_PASSPHRASE }}",
      "PGP_SECRET" -> "${{ secrets.PGP_SECRET }}",
      "SONATYPE_PASSWORD" -> "${{ secrets.SONATYPE_PASSWORD }}",
      "SONATYPE_USERNAME" -> "${{ secrets.SONATYPE_USERNAME }}"
    )
  )
)

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
      )
    )
  )

lazy val mimaSettings = Seq(
  // remove scala 3 exclusion after releasing for Scala 3.0.0
  mimaPreviousArtifacts := {
    if (publishArtifact.value && !scalaVersion.value.startsWith("3")) {
      Set(organization.value %% moduleName.value % (ThisBuild / previousStableVersion).value.get)
    } else Set()
  },
  mimaBinaryIssueFilters ++= {
    import com.typesafe.tools.mima.core._
    // format: off
    Seq(
      ProblemFilters.exclude[Problem]("fs2.kafka.internal.*"),
      ProblemFilters.exclude[IncompatibleSignatureProblem]("*")
    )
    // format: on
  }
)

lazy val noPublishSettings =
  publishSettings ++ Seq(
    publish / skip := true,
    publishArtifact := false
  )

ThisBuild / scalaVersion := scala213
ThisBuild / crossScalaVersions := Seq(scala212, scala213, scala3)

lazy val scalaSettings = Seq(
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
  Compile / doc / scalacOptions += "-nowarn", // workaround for https://github.com/scala/bug/issues/12007
  Compile / console / scalacOptions --= Seq("-Xlint", "-Ywarn-unused"),
  Test / console / scalacOptions := (Compile / console / scalacOptions).value,
  Compile / unmanagedSourceDirectories ++=
    Seq(
      baseDirectory.value / "src" / "main" / {
        if (scalaVersion.value.startsWith("2.12"))
          "scala-2.12"
        else "scala-2.13+"
      }
    ),
  Test / fork := true
)

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

val latestVersion = settingKey[String]("Latest stable released version")
ThisBuild / latestVersion := {
  val snapshot = (ThisBuild / isSnapshot).value
  val stable = (ThisBuild / isVersionStable).value

  if (!snapshot && stable) {
    (ThisBuild / version).value
  } else {
    (ThisBuild / previousStableVersion).value.get
  }
}

val updateSiteVariables = taskKey[Unit]("Update site variables")
ThisBuild / updateSiteVariables := {
  val file =
    (LocalRootProject / baseDirectory).value / "website" / "variables.js"

  val variables =
    Map[String, String](
      "organization" -> (LocalRootProject / organization).value,
      "coreModuleName" -> (core / moduleName).value,
      "latestVersion" -> (ThisBuild / latestVersion).value,
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

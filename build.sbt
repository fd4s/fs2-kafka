organization := "com.ovoenergy"
bintrayOrganization := Some("ovotech")
licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.txt"))

scalaVersion := "2.12.7"
crossScalaVersions := Seq(scalaVersion.value, "2.11.12")
releaseCrossBuild := true

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
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

libraryDependencies ++= Seq(
  "co.fs2" %% "fs2-core" % "1.0.0",
  "org.apache.kafka" % "kafka-clients" % "2.0.0"
)

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.5",
  "org.scalacheck" %% "scalacheck" % "1.14.0",
  "net.manub" %% "scalatest-embedded-kafka" % "2.0.0"
).map(_ % Test)

logBuffered in Test := false
parallelExecution in Test := false
testOptions in Test += Tests.Argument("-oDF")

def addCommandsAlias(name: String, values: List[String]) =
  addCommandAlias(name, values.mkString(";", ";", ""))

addCommandsAlias("validate", List("coverage", "test", "coverageReport"))

name := "Mohammed_Siddiq_HW3"

version := "0.1"

scalaVersion := "2.12.8"

lazy val commonSettings = Seq(
  organization := "MohammedSiddiq.HW3",
  version := "0.1.0-SNAPSHOT"
)
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
lazy val app = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "RelateAuthors"
  ).
  enablePlugins(AssemblyPlugin)
libraryDependencies ++= Seq("org.slf4j" % "slf4j-api" % "1.7.5",
  "org.slf4j" % "slf4j-simple" % "1.7.5", "com.typesafe" % "config" % "1.3.2", "junit" % "junit" % "4.12", "org.apache.hadoop" % "hadoop-client" % "2.4.0", "org.scala-lang.modules" %% "scala-xml" % "1.0.6",
  "org.scalactic" %% "scalactic" % "3.0.5", "org.scalatest" %% "scalatest" % "3.0.5" % "test", "sax" % "sax" % "2.0.1")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
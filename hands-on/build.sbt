name := "hands-on"

version := "1.0-SNAPSHOT"

scalaVersion := "2.12.17"

libraryDependencies ++=  Seq(
  "org.scala-lang" % "scala-library" % "2.12.17",
  "org.apache.flink" %% "flink-scala" % "1.16.1",
  "org.apache.flink" %% "flink-streaming-scala" % "1.16.1" % "provided",
  "org.slf4j" % "slf4j-api" % "2.0.6" % "provided"
)
name := "LogFileStats"

version := "0.1"

scalaVersion := "3.0.2"

val typesafeConfigVersion = "1.4.1"
val scalacticVersion = "3.2.9"
val sfl4sVersion = "2.0.0-alpha5"
val hadoopVersion = "1.2.1"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % typesafeConfigVersion,
  "org.scalatest" %% "scalatest" % scalacticVersion % Test,
  "org.slf4j" % "slf4j-api" % sfl4sVersion,
  "org.apache.hadoop" % "hadoop-core" % hadoopVersion
)
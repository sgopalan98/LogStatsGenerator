name := "LogFileStats"

version := "0.1"

scalaVersion := "3.0.2"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "1.2.1"
name := "scala-sandbox"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.1",
  "org.apache.spark" %% "spark-streaming" % "2.1.1",
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.2.0"
)
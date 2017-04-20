name := "AkkaSpark"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq (
  "com.typesafe.akka" %% "akka-http" % "10.0.5",
  "org.apache.spark" %% "spark-core" % "2.0.2",
  "org.apache.spark" %% "spark-sql" % "2.0.2",
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.0.0"
)
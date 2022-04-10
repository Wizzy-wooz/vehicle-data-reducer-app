name := "vehicle-data-reducer-app"

version := "1.0-SNAPSHOT"

scalaVersion := "2.12.10"

idePackagePrefix := Some("org.vodzianova")

val sparkVersion = "3.1.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "args4j" % "args4j" % "2.32",
  "org.scalatest" %% "scalatest" % "3.1.0" % Test,
  "com.github.mrpowers" %% "spark-fast-tests" % "1.2.0" % Test
)
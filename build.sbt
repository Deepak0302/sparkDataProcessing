name := "OnlineRetailPOC"
organization := "com.retailpoc"
version := "0.1"
scalaVersion := "2.12.10"
//scalaVersion := "2.11.8"
autoScalaLibrary := false
val sparkVersion = "3.0.0-preview2"
//val sparkVersion = "3.3.1"
//val sparkVersion = "2.3.1"

run / javaOptions ++= Seq("-Dlog4j.configuration=file:resource/log4j.properties","-Dlogfile.name=application","-Dspark.yarn.app.container.log.dir=var/logs/")
run / fork := true

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)


val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)

libraryDependencies ++= sparkDependencies ++  testDependencies
libraryDependencies += "com.crealytics" %% "spark-excel" % "0.13.8"


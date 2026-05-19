error id: file:///C:/Users/sbucu/p2team3/P2Team3/enrichscala/build.sbt:local6
file:///C:/Users/sbucu/p2team3/P2Team3/enrichscala/build.sbt
empty definition using pc, found symbol in pc: 
empty definition using semanticdb
empty definition using fallback
non-local guesses:
	 -Dependencies.ClassLoaderLayeringStrategy.
	 -ClassLoaderLayeringStrategy.
	 -scala/Predef.ClassLoaderLayeringStrategy.
offset: 602
uri: file:///C:/Users/sbucu/p2team3/P2Team3/enrichscala/build.sbt
text:
```scala
import Dependencies._

ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.example"
ThisBuild / organizationName := "example"

val sparkVersion = "3.5.0"

lazy val root = (project in file("."))
  .settings(
    name := "project1",

    libraryDependencies ++= Seq(
      munit % Test,
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion
    ),

    Compile / mainClass := Some("example.EnrichData"),

    fork := true,

    classLoaderLayeringStrategy := Class@@LoaderLayeringStrategy.Flat,

    javaOptions ++= Seq(
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
    )
  )
```


#### Short summary: 

empty definition using pc, found symbol in pc: 
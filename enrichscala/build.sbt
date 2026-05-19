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
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "io.github.cdimascio" % "dotenv-java" % "3.0.0"
    ),

    Compile / mainClass := Some("example.EnrichData"),

    fork := true,

    classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat,

    javaOptions ++= Seq(
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
    )
  )
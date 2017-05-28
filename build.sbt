name := "spark-showcase"

version := "0.1.0"

scalaVersion := "2.11.8"

// scalacOptions += "-feature"

libraryDependencies ++=
  Seq(
    "org.slf4j" % "slf4j-api" % "1.7.22",
    "org.slf4j" % "slf4j-log4j12" % "1.7.22",
    "org.apache.spark" %% "spark-core" % "2.1.1",
    "org.apache.spark" %% "spark-sql" % "2.1.1",
    "org.json4s" %% "json4s-native" % "3.5.2",
    "org.scalatest" %% "scalatest" % "3.0.1" % "test"
    // "com.alibaba" % "fastjson" % "1.2.31",
  )

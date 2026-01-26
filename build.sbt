ThisBuild / scalaVersion     := "2.12.19"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.wululu"
ThisBuild / organizationName := "wululu"

lazy val root = (project in file("."))
  .settings(
    name := "data-integration",
    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % "0.7.29" % Test,
      "org.apache.spark" %% "spark-sql" % "3.0.1",
      "org.postgresql" % "postgresql" % "42.7.3",
      "com.oracle.database.jdbc" % "ojdbc8" % "23.3.0.23.09",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.0.1"
    )
  )
name := "Main"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.12.10"

/*lazy val root = (project in file("."))
  .settings(
    name := "Main"
  )*/

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.2.1"


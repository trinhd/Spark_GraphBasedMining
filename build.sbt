lazy val root = (project in file("."))
  .settings(
    name         := "GraphBasedMining",
    organization := "uit.islab.tringuyen",
    scalaVersion := "2.11.8",
    version      := "0.1",
    libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.1"
  )

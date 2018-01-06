lazy val root = (project in file("."))
  .settings(
    name         := "GraphBasedMining",
    organization := "uit.islab.tringuyen",
    scalaVersion := "2.11.11",
    version      := "0.1",
    libraryDependencies ++= Seq(
    	"com.tinkerpop.blueprints" % "blueprints-core" % "2.6.0",
    	"org.apache.spark" % "spark-core_2.11" % "2.1.1",
    	"com.orientechnologies" % "orientdb-core" % "2.2.30",
    	"com.orientechnologies" % "orientdb-graphdb" % "2.2.30",
    	"com.orientechnologies" % "orientdb-client" % "2.2.30"
    )
  )

import sbt._
import Keys._

import java.io._


object Helenos extends Build{

  val scalaVersions = Seq("2.9.0","2.8.1","2.9.1")

  val buildSettings = Defaults.defaultSettings ++ Seq(
    name := "com.geishatokyo.helenos",
    organization := "com.geishatokyo",
    scalaVersion := scalaVersions.head,
    crossScalaVersions := scalaVersions
    //Switch log level to debug
    //logLevel := Level.Debug
  )

  val cassandraThrift = "org.apache.cassandra" % "cassandra-thrift" % "1.0.1"

  val slf4j = "org.slf4j" % "slf4j-log4j12" % "1.6.4" % "provided"

  val junit = "junit" % "junit" % "4.8.1" % "test"
  val specs = (scalaVersion : String ) => {
    val version = scalaVersion match{
      case "2.9.1" => "1.6.9"
      case _ => "1.6.8"
    }
    "org.scala-tools.testing" %% "specs" % version % "test"
  }

  lazy val dependencies = Seq(
    cassandraThrift,
    junit
  )

  val updatePom = TaskKey[Unit]("update-pom")

  val updatePomTask = updatePom <<= makePom map{ file => {
    val moveTo = new java.io.File("pom.xml")
    if(moveTo.exists()){
      println("Delete " + moveTo.getAbsolutePath())
      moveTo.delete()
    }
    println("Move from %s to %s".format(file,moveTo.getAbsolutePath))
    file.renameTo(moveTo)
  }}

  lazy val tasks = Seq(updatePomTask)

  import scala.xml._

  def additionalPom : NodeSeq = {
    XML.loadFile(file("project/pomExtra.xml")).child
  }


  lazy val root = Project("root",file("."),
    settings = buildSettings ++
      Seq(libraryDependencies ++=  dependencies,
          libraryDependencies <<= (scalaVersion, libraryDependencies) { (sv,deps) => {
            deps :+ specs(sv)
          }} ,
          pomExtra := additionalPom) ++
      tasks)
}
import sbt._
import Keys._

object GraphsBuild extends Build {

//	lazy val scCore = ProjectRef(uri("git://github.com/uzh/signal-collect.git#scala-2.11-akka-2.3.2-migration"), "signal-collect")

	lazy val scCore = ProjectRef(file("../signal-collect"), "signal-collect")

  val scYarn = Project(id = "signal-collect-yarn",
    base = file(".")) dependsOn (scCore)
}

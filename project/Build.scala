import sbt._
import Keys._

object GraphsBuild extends Build {
  lazy val scCore = ProjectRef(uri("git://github.com/uzh/signal-collect.git"), id = "signal-collect")
  val scYarn = Project(id = "signal-collect-yarn",
    base = file(".")) dependsOn (scCore)
}

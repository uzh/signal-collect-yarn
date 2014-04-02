import sbt._
import Keys._

object GraphsBuild extends Build {
  lazy val scCore = ProjectRef(file("../signal-collect"), id = "signal-collect")
  val scYarn = Project(id = "signal-collect-yarn",
    base = file(".")) dependsOn (scCore)
}

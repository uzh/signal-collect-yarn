import sbt._
import Keys._

object GraphsBuild extends Build {
  lazy val scCore = ProjectRef(uri("git://github.com/uzh/signal-collect.git"), "signal-collect")
  val scYarn = Project(id = "signal-collect-yarn",
    base = file(".")) dependsOn (scCore)
  unmanagedJars in Compile += file("lib/hadoop-yarn-server-tests-2.2.0-tests.jar")
}

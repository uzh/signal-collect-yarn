import sbt._
import Keys._

object GraphsBuild extends Build {
	lazy val scCore = ProjectRef(uri("git://github.com/uzh/signal-collect.git"), "signal-collect")
//	lazy val scCore1 = ProjectRef(file("../hadoop-2.3.0-src/hadoop-common-project/hadoop-common"), id = "hadoop-common")
//	lazy val scCore2 = ProjectRef(file("../hadoop-2.3.0-src/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common"), id = "hadoop-yarn-common")
//	lazy val scCore3 = ProjectRef(file("../hadoop-2.3.0-src/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client"), id = "hadoop-yarn-client")
//	lazy val scCore4 = ProjectRef(file("../hadoop-2.3.0-src/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager"), id = "hadoop-yarn-server-resourcemanager")
//	lazy val scCore5 = ProjectRef(file("../hadoop-2.3.0-src/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager"), id = "hadoop-yarn-server-nodemanager")
  val scYarn = Project(id = "signal-collect-yarn",
    base = file(".")) dependsOn (scCore) // dependsOn (scCore1) dependesOn (scCore2) dependesOn (scCore3) dependesOn (scCore4) dependesOn (scCore5)
}

import AssemblyKeys._ 
assemblySettings

/** Project */
name := "signal-collect-yarn"

version := "1.0-SNAPSHOT"

organization := "com.signalcollect"

scalaVersion := "2.10.3"

net.virtualvoid.sbt.graph.Plugin.graphSettings

scalacOptions ++= Seq("-optimize", "-Yinline-warnings", "-feature", "-deprecation", "-Xelide-below", "INFO" )

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

EclipseKeys.withSource := true

parallelExecution in Test := false

test in assembly := {}

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("org", "apache", "hadoop", xs @ _*) => MergeStrategy.last
    case x => old(x)
  }
}

excludedJars in assembly <<= (fullClasspath in assembly) map { cp => 
  cp filter { entry =>
    (entry.data.getName == "minlog-1.2.jar" ||
     entry.data.getName == "asm-3.2.jar" ||
     entry.data.getName == "asm-3.1.jar"
   )}
}

/** Dependencies */
libraryDependencies ++= Seq(
  ("org.apache.hadoop" % "hadoop-common" % "2.2.0" % "compile").
    exclude("commons-beanutils", "commons-beanutils-core").
    exclude("commons-collections", "commons-collections"),
  "org.apache.hadoop" % "hadoop-yarn-common" % "2.2.0" % "provided",
  ("org.apache.hadoop" % "hadoop-yarn-client" % "2.2.0" % "compile").
  exclude("hadoop-yarn-api", "org.apache.hadoop"),
  //"org.apache.hadoop" % "hadoop-yarn-server-tests" % "2.2.0" % "test",
  "commons-collections" % "commons-collections" % "3.2.1" % "test",
  "org.apache.hadoop" % "hadoop-minicluster" % "2.2.0" % "test",
  "junit" % "junit" % "4.8.2"  % "test",
  "org.specs2" %% "specs2" % "2.3.3"  % "test"
)
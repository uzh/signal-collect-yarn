import AssemblyKeys._ 
assemblySettings

/** Project */
name := "signal-collect-yarn"

version := "1.0-SNAPSHOT"

organization := "com.signalcollect"

scalaVersion := "2.10.3"

val hadoopVersion = "2.2.0"

net.virtualvoid.sbt.graph.Plugin.graphSettings

scalacOptions ++= Seq("-optimize", "-Yinline-warnings", "-feature", "-deprecation", "-Xelide-below", "INFO" )

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

EclipseKeys.withSource := true

parallelExecution in Test := false

test in assembly := {}

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("org", "apache", "hadoop", xs @ _*) => MergeStrategy.last
    case PathList("org", "apache", "commons", "collections", xs @ _*) => MergeStrategy.last
    case PathList("org", "objectweb", "asm", xs @ _*) => MergeStrategy.last
    case PathList("com", "thoughtworks", xs @ _*) => MergeStrategy.last
    case PathList("META-INF", "maven", xs @ _*) => MergeStrategy.last
    case PathList("log4j.properties") => MergeStrategy.last
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
  "com.google.protobuf" % "protobuf-java" % "2.4.1" force(),
  "org.scala-lang.modules" %% "scala-async" % "0.9.0",
  "org.scala-lang" % "scala-library" % "2.10.3" % "compile",
//  ("org.apache.hadoop" % "hadoop-common" % hadoopVersion % "provided").
//    exclude("commons-beanutils", "commons-beanutils-core"),
//  "org.apache.hadoop" % "hadoop-yarn-common" % hadoopVersion % "provided",
//  ("org.apache.hadoop" % "hadoop-yarn-client" % hadoopVersion % "provided").
//  exclude("hadoop-yarn-api", "org.apache.hadoop"),
// "org.apache.hadoop" % "hadoop-yarn-server-resourcemanager" % hadoopVersion % "provided",
//  "org.apache.hadoop" % "hadoop-yarn-server-nodemanager" % hadoopVersion % "provided",
  "junit" % "junit" % "4.8.2"  % "test",
  "org.specs2" %% "specs2" % "2.3.3"  % "test",
  "com.typesafe.akka" %% "akka-remote" % "2.1.4" force(),
  "io.netty" % "netty" % "3.5.8.Final" force()
)

dependencyOverrides += "io.netty" % "netty" % "3.5.8.Final"

dependencyOverrides += "com.google.protobuf" % "protobuf-java" % "2.4.1"
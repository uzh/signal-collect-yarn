import AssemblyKeys._ 
assemblySettings

/** Project */
name := "signal-collect-yarn"

version := "1.0-SNAPSHOT"

organization := "com.signalcollect"

scalaVersion := "2.11.0"

val hadoopVersion = "2.3.0"

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
  "org.scala-lang.modules" %% "scala-async" % "0.9.1",
  "org.scala-lang" % "scala-library" % "2.11.0" % "compile",
 ("org.apache.hadoop" % "hadoop-common-custom" % hadoopVersion % "compile").
   exclude("commons-beanutils", "commons-beanutils-core"),
  "org.apache.hadoop" % "hadoop-yarn-common-custom" % hadoopVersion % "compile",
  ("org.apache.hadoop" % "hadoop-yarn-client-custom" % hadoopVersion % "compile").
  exclude("hadoop-yarn-api", "org.apache.hadoop"),
 "org.apache.hadoop" % "hadoop-yarn-server-resourcemanager-custom" % hadoopVersion % "compile",
  "org.apache.hadoop" % "hadoop-yarn-server-nodemanager-custom" % hadoopVersion % "compile",
  "org.apache.hadoop" % "yarn-server-tests-custom" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-hdfs-custom" % hadoopVersion,
  "com.github.romix.akka" %% "akka-kryo-serialization-custom" % "0.3.4" % "compile",
  "log4j" % "log4j" % "1.2.17" % "compile",
  "junit" % "junit" % "4.8.2"  % "test",
  "org.specs2" %% "specs2" % "2.3.11"  % "test",
  "com.typesafe.akka" %% "akka-remote" % "2.3.2" force(),
  "io.netty" % "netty" % "3.5.8.Final" force()
)

resolvers += "Ifi Public" at "https://maven.ifi.uzh.ch/maven2/content/groups/public/"

dependencyOverrides += "io.netty" % "netty" % "3.5.8.Final"

dependencyOverrides += "com.google.protobuf" % "protobuf-java" % "2.4.1"
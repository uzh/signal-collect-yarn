import AssemblyKeys._ 
assemblySettings

/** Project */
name := "signal-collect-yarn"

version := "1.0-SNAPSHOT"

organization := "com.signalcollect"

scalaVersion := "2.10.3"

scalacOptions ++= Seq("-optimize", "-Yinline-warnings", "-feature", "-deprecation", "-Xelide-below", "INFO" )

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

EclipseKeys.withSource := true

test in assembly := {}

parallelExecution in Test := false


/** Dependencies */
libraryDependencies ++= Seq(
  )

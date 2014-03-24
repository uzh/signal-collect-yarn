package com.signalcollect.yarn.deployment;

import scala.sys.process._

object LocalJarRunner {
  
  def run(classpath: String, mainClass: String) {
    val completeClasspath = classpath + ":/home/tobi/Dropbox/uni/Bachelorarbeit/workspace/signal-collect-yarn/target/scala-2.10/signal-collect-yarn-assembly-1.0-SNAPSHOT.jar"
    val command = Seq("java", "-cp", completeClasspath, mainClass)
    command !! (ProcessLogger(println(_)))
  }

}


package com.signalcollect.yarn.deployment

import scala.sys.process._

object LocalJarRunner {
  
  def run(classpath: String, mainClass: String, arguments: String = "") {
    val completeClasspath = classpath +  ":./target/scala-2.10/signal-collect-yarn-assembly-1.0-SNAPSHOT.jar"
    val command = Seq("java", "-cp", completeClasspath, mainClass, arguments," 1> /home/tobi/log1.txt")
    command !! (ProcessLogger(println(_)))
  }

}


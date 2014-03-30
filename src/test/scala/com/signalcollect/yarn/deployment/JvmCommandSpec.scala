package com.signalcollect.yarn.deployment

import org.junit.runner.RunWith
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.runner.JUnitRunner
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.ApplicationConstants


@RunWith(classOf[JUnitRunner])
class JvmCommandSpec extends SpecificationWithJUnit {
 "JvmCommand" should {
//   val mainClass = "MainClass"
//   "generate Command" in {
//     val settings = new LaunchSettings(memory = 128,
//         mainClass = mainClass,
//         pathsToJars = List("first.jar"))
//     
//     val command = new JvmCommand(settings).getCommand
//     
//    command === Environment.JAVA_HOME.$() + "/bin/java" +
//      " -cp first.jar" +
//      " -Xmx128M MainClass " + 
//      " 1> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/MainClass.stdout" + 
//      " 2> <LOG_DIR>/MainClass.stderr"
//   }
//   
//   "generate Command with multiple jars in classpath" in {
//     val jarsForClasspath = List("first.jar", "second.jar")
//     
//     val settings =  new LaunchSettings(pathsToJars = jarsForClasspath, mainClass = mainClass)
//     
//     val command = new JvmCommand(settings).getCommand
//     
//     command === Environment.JAVA_HOME.$() + "/bin/java" +
//      " -cp first.jar:second.jar" +
//      " -Xmx128M MainClass " + 
//      " 1> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/MainClass.stdout" + 
//      " 2> <LOG_DIR>/MainClass.stderr"
//   }
 }
}
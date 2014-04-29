/*
 *  @author Tobias Bachmann
 *
 *  Copyright 2014 University of Zurich
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
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
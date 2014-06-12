/**
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
package com.signalcollect.deployment.yarn

import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment

import com.signalcollect.util.ConfigProvider
import com.signalcollect.util.LogHelper

import scala.collection.immutable.List

class JvmCommand(settings: LaunchSettings) extends LogHelper {
  val javaHome = settings.javaHome
  val classpath = settings.classpath
  val memory = settings.memory
  val logDir = settings.logDir
  val mainClass = settings.mainClass
  val arguments: String = settings.arguments.mkString(" ")
  val jvmArguments = settings.jvmArguments

  def getCommand: String = {

    val command = s"${javaHome}/bin/java -cp $classpath" +
      s" -Xmx${memory}M -Xms${memory}m $jvmArguments  $mainClass $arguments" +
      s" 1> ${logDir}/${mainClass}.stdout" +
      s" 2> ${logDir}/${mainClass}.stderr"
    log.info("launchCommand for Container created:" + command)
    command
    
  }
}

case class LaunchSettings(val memory: Int,
  val mainClass: String = ConfigProvider.config.getString("deployment.mainClass"),
  val pathsToJars: List[String] = List(ConfigProvider.config.getString("deployment.pathToJar")),
  val arguments: List[String] = List[String](),
  val useDefaultYarnClientCreator: Boolean = false,
  val jvmArguments: String = "-XX:+AggressiveOpts -XX:+AlwaysPreTouch -XX:+UseNUMA -XX:-UseBiasedLocking -XX:MaxInlineSize=1024",
  val javaHome: String = Environment.JAVA_HOME.$(),
  val classpath: String ,
  val logDir: String = ApplicationConstants.LOG_DIR_EXPANSION_VAR,
  val filesOnHdfs: List[String]) {
  
  
//  private def createClassPath(): String = {
//    val jarNames = pathsToJars.map(_.split("/").last).mkString(":")
//  }
}
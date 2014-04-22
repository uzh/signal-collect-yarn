package com.signalcollect.yarn.deployment

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

  def getCommand: String = {

    val command = s"${javaHome}/bin/java -cp $classpath" +
      s" -Xmx${memory}M -XX:+AggressiveOpts -XX:+AlwaysPreTouch -XX:+UseNUMA -XX:-UseBiasedLocking -XX:MaxInlineSize=1024 $mainClass $arguments" +
      s" 1> ${logDir}/${mainClass}.stdout" +
      s" 2> ${logDir}/${mainClass}.stderr"
    log.info("launchCommand for Container created:" + command)
    command
    
  }
}

class LaunchSettings(val memory: Int = ConfigProvider.config.getInt("deployment.memory"),
  val mainClass: String = ConfigProvider.config.getString("deployment.mainClass"),
  val pathsToJars: List[String] = List(ConfigProvider.config.getString("deployment.pathToJar")),
  val arguments: List[String] = List[String](),
  val useDefaultYarnClientCreator: Boolean = false) {
  val javaHome = Environment.JAVA_HOME.$()
  val classpath = createClassPath
  val logDir = ApplicationConstants.LOG_DIR_EXPANSION_VAR
  
  private def createClassPath(): String = {
    val jarNames = pathsToJars.map(_.split("/").last)
    jarNames.mkString(":")
  }
}
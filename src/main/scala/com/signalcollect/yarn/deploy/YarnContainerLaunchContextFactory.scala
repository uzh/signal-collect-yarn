package com.signalcollect.yarn.deploy

import com.signalcollect.util.ConfigProvider
import org.apache.hadoop.yarn.util.Records
import com.signalcollect.util.JarUploader
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records._
import java.util.HashMap
import scala.collection.JavaConversions._
import com.signalcollect.util.LogHelper
import com.typesafe.config.Config

class YarnContainerLaunchContextFactory(
    mainClass: String = ConfigProvider.config.getString("deployment.mainClass"),
    pathToJar: String = ConfigProvider.config.getString("deployment.pathToJar"),
    jarName: String = ConfigProvider.config.getString("deployment.jarName")) extends LogHelper {

  val config = ConfigProvider.config
  val memory = config.getInt("deployment.memory")
  val javaHome = Environment.JAVA_HOME.$()
  val jarCreator = new JarCreator()
  val logDir = ApplicationConstants.LOG_DIR_EXPANSION_VAR
  
  def createLaunchContext(applicationId: String): ContainerLaunchContext = {
    val launchContext = Records.newRecord(classOf[ContainerLaunchContext])
    val jarResource = createLocalResourceForJar(applicationId)
    launchContext.setLocalResources(jarResource)

    val commands = createCommand
    launchContext.setCommands(commands)
    launchContext
  }

  private def createCommand: List[String] = {

    val command = s"${javaHome}/bin/java -cp $jarName" +
      s" -Xmx${memory}M $mainClass" +
      s" 1> ${logDir}/${jarName}.stdout" +
      s" 2> ${logDir}/${jarName}.stderr"

    log.info("Completed setting up app master command " + command)
    List(command)
  }

  private def createLocalResourceForJar(applicationId: String): HashMap[String, LocalResource] = {
    val uploader = new JarUploader(applicationId, pathToJar = pathToJar)
    uploader.uploadJar()
  }
}
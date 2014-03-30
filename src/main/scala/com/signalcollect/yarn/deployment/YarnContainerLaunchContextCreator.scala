package com.signalcollect.yarn.deployment

import java.util.HashMap

import scala.collection.JavaConversions._

import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.util.Records

import com.signalcollect.util.ConfigProvider
import com.signalcollect.util.JarUploader
import com.signalcollect.util.JarUploader
import com.typesafe.config.Config

class YarnContainerLaunchContextCreator(launchSettings: LaunchSettings) {
  
  def createLaunchContext(applicationId: String): ContainerLaunchContext = {
    val launchContext = Records.newRecord(classOf[ContainerLaunchContext])
    val jarResource = createLocalResourceForJar(applicationId)
    launchContext.setLocalResources(jarResource)

    val commands = createCommand
    launchContext.setCommands(commands)
    launchContext
  }

  private def createCommand: List[String] = {
    val jvmCommand = new JvmCommand(launchSettings)
    val command = jvmCommand.getCommand

    List(command)
  }

  private def createLocalResourceForJar(applicationId: String): HashMap[String, LocalResource] = {
    val uploader = new JarUploader(applicationId, launchSettings.pathsToJars)
    uploader.uploadJars()
  }
}
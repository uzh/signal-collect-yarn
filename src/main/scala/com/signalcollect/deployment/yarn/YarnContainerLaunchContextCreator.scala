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

import java.util.HashMap
import scala.collection.JavaConversions._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.util.Records
import com.signalcollect.util.FileUploader
import com.signalcollect.util.FileUploader
import com.signalcollect.logging.Logging

/**
 * create a ContainerLaunchContext. The ContainerLaunchContext defines which jar file, and which files are deployed and started.
 */
class YarnContainerLaunchContextCreator(launchSettings: LaunchSettings,
    filesAlreadyInContainer: List[String] = Nil,
    deployConfig: YarnDeploymentConfiguration) extends Logging {
  def createLaunchContext(applicationId: String): ContainerLaunchContext = {
    val uploader = new FileUploader(applicationId, launchSettings.pathsToJars, launchSettings.useDefaultYarnClientCreator, deployConfig)
    val launchContext = Records.newRecord(classOf[ContainerLaunchContext])
    val jarResource = uploader.uploadFiles()
    val containerFiles = filesAlreadyInContainer.map(uploader.getPathOnFs(_))
    log.debug(s"containerFiles: $containerFiles")
    val filesOnHdfs = launchSettings.filesOnHdfs ::: containerFiles
    log.debug(s"filesOnHdfs: $filesOnHdfs")
    val hdfsFiles: List[(String, LocalResource)] = filesOnHdfs.map(uploader.createLocalResource(_))
    hdfsFiles.foreach(file => jarResource.put(file._1 , file._2 ))
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

}
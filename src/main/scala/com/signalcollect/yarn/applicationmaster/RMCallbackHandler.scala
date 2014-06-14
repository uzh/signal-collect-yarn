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
package com.signalcollect.yarn.applicationmaster

import java.io.File
import scala.collection.JavaConversions._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync
import org.apache.hadoop.yarn.client.api.async.NMClientAsync
import com.signalcollect.util.LogHelper
import com.signalcollect.util.ConfigProvider
import java.net.InetAddress
import com.signalcollect.deployment.yarn.YarnContainerLaunchContextCreator
import com.signalcollect.deployment.yarn.LaunchSettings
import com.signalcollect.deployment.DeploymentConfiguration

class RMCallbackHandler(nodeManagerClient: NMClientAsync, deploymentConfig: DeploymentConfiguration, applicationId: String) extends AMRMClientAsync.CallbackHandler with LogHelper {

  override def onContainersCompleted(completedContainers: java.util.List[ContainerStatus]): Unit = {
    log.info("Got response from RM for container ask, completedCnt="
      + completedContainers.size)
      Thread.sleep(1000)
      val allSuccessfull = completedContainers.forall(_.getExitStatus() == 0)
      ContainerRegistry.setSuccessfull(allSuccessfull) 
      ContainerRegistry.setFinished(completedContainers.size)
  }

  override def onContainersAllocated(allocatedContainers: java.util.List[Container]) {
    log.info("Got response from RM for container ask, allocatedCnt="
      + allocatedContainers.size)
    allocatedContainers.foreach(startContainer(_))
  }

  override def onShutdownRequest() {
    log.info("shutdownRequest")
  }

  override def onNodesUpdated(updatedNodes: java.util.List[NodeReport]) {}

  override def getProgress(): Float = {
    0.1f
  }

  override def onError(e: Throwable) {
    log.info("onError")
  }

  private def startContainer(container: Container) = {
    val config = ConfigProvider.config
    val containerId = ContainerRegistry.register(container)
    val leaderIp = InetAddress.getLocalHost().getHostAddress()
    val copyFiles = deploymentConfig.copyFiles.map(_.split("/").last)
    val dependencyOnHdfs = config.getBoolean("testing.onHdfs")
    val files = getJarAndConfFilesInCurrentDir.filter(!_.contains("signal-collect-yarn-assembly-1.0-SNAPSHOT") || !dependencyOnHdfs) ::: copyFiles
    val filesOnHdfs = config.getStringList("deployment.files-on-hdfs").toList
    val launchSettings = new LaunchSettings(
      pathsToJars = Nil,
      arguments = List[String](containerId.toString,leaderIp),
      memory = deploymentConfig.memoryPerNode,
      useDefaultYarnClientCreator = true,
      filesOnHdfs = filesOnHdfs,
      classpath = (getJarAndConfFilesInCurrentDir ::: filesOnHdfs).mkString(":"))
    val launchContextCreator = new YarnContainerLaunchContextCreator(launchSettings, files)
    val ctx = launchContextCreator.createLaunchContext(applicationId)
    nodeManagerClient.startContainerAsync(container, ctx)
  }

  private def getJarAndConfFilesInCurrentDir(): List[String] = {
    val currentFolder = new File("./")
    val files = currentFolder.listFiles.toList
    val allFiles = files.filter(file => file.getAbsolutePath().endsWith(".conf") || file.getAbsolutePath().endsWith(".jar"))
    val paths = allFiles.map(_.toString)
    println(s"jarfiles to uploade are: $paths")
    paths
  }
}
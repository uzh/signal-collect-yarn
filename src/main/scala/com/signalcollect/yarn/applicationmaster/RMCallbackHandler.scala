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
import java.net.InetAddress
import scala.collection.JavaConversions.asScalaBuffer
import org.apache.hadoop.yarn.api.records.Container
import org.apache.hadoop.yarn.api.records.ContainerStatus
import org.apache.hadoop.yarn.api.records.NodeReport
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync
import org.apache.hadoop.yarn.client.api.async.NMClientAsync
import com.signalcollect.deployment.DeploymentConfiguration
import com.signalcollect.deployment.Leader
import com.signalcollect.deployment.yarn.LaunchSettings
import com.signalcollect.deployment.yarn.YarnContainerLaunchContextCreator
import com.signalcollect.logging.Logging
import com.signalcollect.deployment.yarn.YarnDeploymentConfiguration
/**
 * CallbackHandler for the RessourcManager, when containers are started it submits the application to them
 */
class RMCallbackHandler(nodeManagerClient: NMClientAsync, deploymentConfig: YarnDeploymentConfiguration, applicationId: String, leader: Leader) extends AMRMClientAsync.CallbackHandler with Logging {

  override def onContainersCompleted(completedContainers: java.util.List[ContainerStatus]): Unit = {
    log.info("Got response from RM for container ask, completedCnt="
      + completedContainers.size)
      Thread.sleep(1000)
      completedContainers.foreach( c => println("diagnostics for container with id:" + c.getContainerId() + c.getDiagnostics()))
      println(completedContainers.map("exit status" + _.getExitStatus().toString))
      val allSuccessfull = completedContainers.forall(_.getExitStatus() == 0)
      ContainerRegistry.setSuccessfull(allSuccessfull) 
      ContainerRegistry.setFinished(completedContainers.size)
  }

  override def onContainersAllocated(allocatedContainers: java.util.List[Container]) {
    log.info("Got response from RM for container ask, allocatedCnt="
      + allocatedContainers.size)
    allocatedContainers.foreach(startContainer(_))
    allocatedContainers.foreach(createLogUrl(_))
  }
  
  def createLogUrl(container: Container){
    println("logUrl: ")
    println(container.getNodeHttpAddress() + "/node/containerlogs/" + container.getId())
  }

  override def onShutdownRequest() {
    log.info("shutdownRequest")
  }

  override def onNodesUpdated(updatedNodes: java.util.List[NodeReport]) {}

  override def getProgress(): Float = {
    0.1f
  }

  override def onError(e: Throwable) {
    println("on Error")
    throw e
  }

  private def startContainer(container: Container) = {
    val containerId = ContainerRegistry.register(container)
    val leaderIp = InetAddress.getLocalHost().getHostAddress()
    val copyFiles = deploymentConfig.copyFiles.map(_.split("/").last)
    val dependencyOnHdfs = deploymentConfig.testDependenciesOnHdfs 
    val files = getJarAndConfFilesInCurrentDir ::: copyFiles
    val filesOnHdfs = deploymentConfig.filesOnHdfs
    val launchSettings = new LaunchSettings(
      jvmArguments = deploymentConfig.jvmArguments,
      mainClass = deploymentConfig.containerClass,
      pathsToJars = Nil,
      arguments = List[String](containerId.toString,leaderIp),
      memory = deploymentConfig.memoryPerNode,
      useDefaultYarnClientCreator = true,
      filesOnHdfs = filesOnHdfs,
      classpath = (getJarAndConfFilesInCurrentDir ::: filesOnHdfs).mkString(":"))
    val launchContextCreator = new YarnContainerLaunchContextCreator(launchSettings, files, deploymentConfig)
    val ctx = launchContextCreator.createLaunchContext(applicationId)
    nodeManagerClient.startContainerAsync(container, ctx)
  }

  /**
   * get the files in current directory, which has to be sent to the containers
   */
  private def getJarAndConfFilesInCurrentDir(): List[String] = {
    val currentFolder = new File("./")
    val files = currentFolder.listFiles.toList
    val allFiles = files.filter(file => file.getAbsolutePath().endsWith(".conf") || file.getAbsolutePath().endsWith(".jar"))
    val paths = allFiles.map(_.toString)
    paths
  }
}
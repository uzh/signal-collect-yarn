package com.signalcollect.yarn.applicationmaster

import java.io.File
import scala.collection.JavaConversions._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync
import org.apache.hadoop.yarn.client.api.async.NMClientAsync
import com.signalcollect.util.LogHelper
import com.signalcollect.util.ConfigProvider
import com.signalcollect.yarn.deployment.LaunchSettings
import com.signalcollect.yarn.deployment.YarnContainerLaunchContextCreator
import java.net.InetAddress

class RMCallbackHandler(nodeManagerClient: NMClientAsync) extends AMRMClientAsync.CallbackHandler with LogHelper {

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
    val containerId = ContainerRegistry.register(container)
    val jarFiles = getJarFilesInCurrentDir()
    val launchSettings = new LaunchSettings(
      pathsToJars = jarFiles,
      arguments = List[String](containerId.toString),
      memory = ConfigProvider.config.getInt("deployment.containerMemory"))
    val launchContextCreator = new YarnContainerLaunchContextCreator(launchSettings)
    val ctx = launchContextCreator.createLaunchContext(container.getId().toString())
    nodeManagerClient.startContainerAsync(container, ctx)
  }

  private def getJarFilesInCurrentDir(): List[String] = {
    val currentFolder = new File("./")
    val files = currentFolder.listFiles.toList
    val jarFiles = files.filter(file => file.getAbsolutePath().endsWith(".jar"))
    val jarPaths = jarFiles.map(_.toString)
    jarPaths
  }
}
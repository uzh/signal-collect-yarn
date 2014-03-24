package com.signalcollect.yarn.applicationmaster

import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync
import com.signalcollect.util.LogHelper
import org.apache.hadoop.yarn.util.Records
import scala.collection.JavaConversions._
import com.signalcollect.util.JarUploader
import com.signalcollect.yarn.deployment.YarnContainerLaunchContextCreator
import org.apache.hadoop.yarn.client.api.async.NMClientAsync
import com.signalcollect.yarn.deployment.LaunchSettings


class RMCallbackHandler(nodeManagerClient: NMClientAsync) extends AMRMClientAsync.CallbackHandler with LogHelper {
	
  override def onContainersCompleted(completedContainers: java.util.List[ContainerStatus]): Unit = {
    log.info("Got response from RM for container ask, completedCnt="
      + completedContainers.size)

  }

  override def onContainersAllocated(allocatedContainers: java.util.List[Container]) {
    log.info("Got response from RM for container ask, allocatedCnt="
      + allocatedContainers.size)
      startContainer(allocatedContainers.get(0))
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
     log.info("Setting up container launch container for containerid="
          + container.getId() + "containerAddress=" + container.getNodeHttpAddress())
      val launchContextCreator = new YarnContainerLaunchContextCreator(
          new LaunchSettings(pathsToJars = List("./")))
      val ctx = launchContextCreator.createLaunchContext(container.getId().toString())
//      containerListener.addContainer(container.getId(), container)
      nodeManagerClient.startContainerAsync(container, ctx)
  }
}
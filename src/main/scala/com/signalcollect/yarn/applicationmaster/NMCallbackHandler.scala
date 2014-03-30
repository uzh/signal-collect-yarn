package com.signalcollect.yarn.applicationmaster

import org.apache.hadoop.yarn.api.records.ContainerStatus
import org.apache.hadoop.yarn.api.records.ContainerId
import org.apache.hadoop.yarn.client.api.async.NMClientAsync
import com.signalcollect.util.LogHelper
import java.nio.ByteBuffer
import org.apache.hadoop.yarn.api.records.ContainerState
import com.signalcollect.nodeprovisioning.yarn.Leader
import com.signalcollect.util.ConfigProvider

class NMCallbackHandler
  extends NMClientAsync.CallbackHandler with LogHelper {
  override def onContainerStopped(containerId: ContainerId) {
    log.info("onContainerStopped")
  }

  override def onContainerStatusReceived(containerId: ContainerId,
    containerStatus: ContainerStatus) {
    log.info("onOntainerStatusReceived")
    if (ContainerState.COMPLETE.equals(containerStatus.getState())) {
      if (containerStatus.getExitStatus() != 0) {
        ContainerRegistry.setSuccessfull(false)
      }
    }
  }

  override def onContainerStarted(containerId: ContainerId,
    allServiceResponse: java.util.Map[String, ByteBuffer]) {
    log.info("onContarineStarted")
    ContainerRegistry.containerStarted()
    if (ContainerRegistry.allStarted) {
      val containerNodes = ContainerRegistry.getContainerNodes
      val numberOfContainers = ConfigProvider.config.getInt("deployment.numberOfNodes")
      val akkaPort = ConfigProvider.config.getInt("deployment.akka.port")
      Thread.sleep(2000) // make sure all other nodes are ready
      val leader = new Leader(containerNodes, akkaPort, List[String]())
      try {
      leader.startExecution
        
      } catch {
        case e: Exception => {
          throw e
        }
      } finally {
        leader.stopExecution
      }

    }
  }

  override def onStartContainerError(containerId: ContainerId, t: Throwable) {
    log.info("onStartContainerError")
    ContainerRegistry.setSuccessfull(false)
  }

  override def onGetContainerStatusError(
    containerId: ContainerId, t: Throwable) {
    log.error("Failed to query the status of Container " + containerId);
    ContainerRegistry.setSuccessfull(false)
  }

  override def onStopContainerError(containerId: ContainerId, t: Throwable) {
    log.error("Failed to stop Container " + containerId)
    ContainerRegistry.setSuccessfull(false)
  }
}

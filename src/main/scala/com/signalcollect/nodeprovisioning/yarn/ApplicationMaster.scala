package com.signalcollect.nodeprovisioning.yarn

import java.nio.ByteBuffer;
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.exceptions.YarnException
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync
import org.apache.hadoop.yarn.client.api.async.NMClientAsync
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl
import org.apache.hadoop.yarn.util.Records
import com.signalcollect.util.LogHelper
import scala.App
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

class ApplicationMaster(config: Configuration = new YarnConfiguration()) extends App with LogHelper {
  run()
  def run() {
    val allocListener = new RMCallbackHandler()
    val amRMClient :AMRMClientAsync[ContainerRequest] = AMRMClientAsync.createAMRMClientAsync(1000, allocListener)
    amRMClient.init(config)
    amRMClient.start()
    val containerListener = new NMCallbackHandler()
    val nmClientAsync = new NMClientAsyncImpl(containerListener)
    nmClientAsync.init(config);
    nmClientAsync.start();
    
    val appMasterHostname = NetUtils.getHostname();
    val response = amRMClient
        .registerApplicationMaster(appMasterHostname, -1, "")
    val containerAsk = setupContainerAskForRM()
    amRMClient.addContainerRequest(containerAsk)
    try{
      Thread.sleep(10000)
    } catch {
      case e: Exception => log.info("interrupted")
    }
    nmClientAsync.stop()
    val appStatus = FinalApplicationStatus.SUCCEEDED
    val appMessage = "success"
    try {
      amRMClient.unregisterApplicationMaster(appStatus, appMessage, null)
    } catch {
      case e: Exception => log.info("failed unregister ApplicationMaster")
    }
    
    amRMClient.stop()
  }
  
  def setupContainerAskForRM(): ContainerRequest = {
    val pri = Records.newRecord(classOf[Priority])
    pri.setPriority(0)

    val capability = Records.newRecord(classOf[Resource])
    capability.setMemory(128)

    val request = new ContainerRequest(capability, null, null, pri)
    log.info("Requested container ask: " + request.toString())
    request
  }
}

class RMCallbackHandler extends AMRMClientAsync.CallbackHandler with LogHelper {
	
  var containers: HashMap[ContainerId, Container] = new HashMap[ContainerId, Container]()
  override def onContainersCompleted(completedContainers: java.util.List[ContainerStatus]): Unit = {
    log.info("Got response from RM for container ask, completedCnt="
      + completedContainers.size)

  }

  override def onContainersAllocated(allocatedContainers: java.util.List[Container]) {
    log.info("Got response from RM for container ask, allocatedCnt="
      + allocatedContainers.size)
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
}

class NMCallbackHandler
    extends NMClientAsync.CallbackHandler with LogHelper {
    override def onContainerStopped(containerId: ContainerId) {
      log.info("onContainerStopped")
    }

    override def onContainerStatusReceived(containerId: ContainerId,
         containerStatus: ContainerStatus) {
      log.info("onOntainerStatusReceived")
    }

    override def onContainerStarted(containerId: ContainerId,
        allServiceResponse: java.util.Map[String, ByteBuffer]) {
      log.info("onContarineStarted")
    }

    override def onStartContainerError(containerId: ContainerId, t: Throwable ) {
      log.info("onStartContainerError")
    }

    override def onGetContainerStatusError(
        containerId: ContainerId, t: Throwable) {
      log.error("Failed to query the status of Container " + containerId);
    }

    override def onStopContainerError(containerId: ContainerId, t: Throwable) {
      log.error("Failed to stop Container " + containerId);
    }
  }

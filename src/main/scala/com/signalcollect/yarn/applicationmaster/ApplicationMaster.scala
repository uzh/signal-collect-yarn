package com.signalcollect.yarn.applicationmaster

import java.nio.ByteBuffer
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync
import org.apache.hadoop.yarn.client.api.async.NMClientAsync
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl
import org.apache.hadoop.yarn.util.Records
import com.signalcollect.util.LogHelper
import scala.collection.JavaConversions._
import org.apache.hadoop.yarn.api.records.Priority
import org.apache.hadoop.yarn.api.records.Resource
import com.signalcollect.yarn.deployment.YarnClientCreator
import java.io.File
import org.apache.hadoop.fs.Path

object ApplicationMaster extends App with LogHelper {
  var config: Configuration = new YarnConfiguration()
  val siteXml = new Path("yarn-site.xml")
  config.addResource(siteXml)
  log.info("RM Address: " + config.get("yarn.resourcemanager.address"))
  val containerListener = new NMCallbackHandler()
  val nodeManagerClient = new NMClientAsyncImpl(containerListener)
  
  val allocListener = new RMCallbackHandler(nodeManagerClient)
  val amRMClient: AMRMClientAsync[ContainerRequest] = AMRMClientAsync.createAMRMClientAsync(1000, allocListener)
  
  run()

  def run() {

    amRMClient.init(config)
    amRMClient.start()
    
    nodeManagerClient.init(config)
    nodeManagerClient.start()

    val appMasterHostname = NetUtils.getHostname();
    val response = amRMClient
      .registerApplicationMaster(appMasterHostname, -1, "")
    val containerAsk = setupContainerAskForRM()
    amRMClient.addContainerRequest(containerAsk)
    try {
      Thread.sleep(10000)
    } catch {
      case e: Exception => log.info("interrupted")
    }
    nodeManagerClient.stop()
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

  override def onStartContainerError(containerId: ContainerId, t: Throwable) {
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

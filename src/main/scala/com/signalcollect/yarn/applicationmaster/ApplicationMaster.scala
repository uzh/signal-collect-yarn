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
import com.signalcollect.util.ConfigProvider

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
    initApplicationMaster
    val containerAsk = setupContainerAskForRM()
    amRMClient.addContainerRequest(containerAsk)
    try {
      while(!ContainerRegistry.isFinished){
        Thread.sleep(100)
      }
    } catch {
      case e: Exception => log.info("interrupted")
    }
    nodeManagerClient.stop()
    val appStatus = if(ContainerRegistry.successfull) FinalApplicationStatus.SUCCEEDED else FinalApplicationStatus.FAILED
    val appMessage = "finished"
    try {
      amRMClient.unregisterApplicationMaster(appStatus, appMessage, null)
    } catch {
      case e: Exception => log.info("failed unregister ApplicationMaster")
    }

    amRMClient.stop()
    System.exit(0) // if there are still some threads running, they are killed by that
  }
  
  private def initApplicationMaster = {
		  amRMClient.init(config)
		  amRMClient.start()
		  
		  nodeManagerClient.init(config)
		  nodeManagerClient.start()
		  
		  val appMasterHostname = NetUtils.getHostname();
		  val response = amRMClient
				  .registerApplicationMaster(appMasterHostname, -1, "")
  }

  def setupContainerAskForRM(): ContainerRequest = {
    val memory = ConfigProvider.config.getInt("deployment.containerMemory")
    val pri = Records.newRecord(classOf[Priority])
    pri.setPriority(0)

    val capability = Records.newRecord(classOf[Resource])
    capability.setMemory(memory)

    val request = new ContainerRequest(capability, null, null, pri)
    log.info("Requested container ask: " + request.toString())
    request
  }
  
}

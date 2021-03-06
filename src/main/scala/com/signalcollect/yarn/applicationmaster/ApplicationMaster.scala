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

import org.apache.hadoop.fs.Path
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus
import org.apache.hadoop.yarn.api.records.Priority
import org.apache.hadoop.yarn.api.records.Resource
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl
import org.apache.hadoop.yarn.util.Records
import com.signalcollect.deployment.LeaderCreator
import com.signalcollect.deployment.yarn.YarnClientCreator
import com.signalcollect.deployment.yarn.YarnDeploymentConfigurationCreator
import com.signalcollect.util.HdfsWrapper
import com.signalcollect.logging.Logging
import com.signalcollect.logging.SocketLogger
import com.signalcollect.util.NodeKiller

/**
 * ApplicationMaster which starts the execution and communicates with the ResourceManager
 */
object ApplicationMaster extends App with Logging {
  val masterIp = args(1)
  val algorithm = args(2)
  val deploymentConfig = YarnDeploymentConfigurationCreator.getYarnDeploymentConfiguration.copy(algorithm = algorithm)
  if(deploymentConfig.killOtherNodes) NodeKiller.killOtherMasterAndNodes
  startLogServer
  log.debug(s"deploymentConfig is $deploymentConfig")
  YarnClientCreator.masterIp = masterIp
  YarnClientCreator.useDefaultCreator(deploymentConfig)

  val config = YarnClientCreator.yarnClient.getConfig()
  val siteXml = new Path("dummy-yarn-site.xml") //this is needed for the minicluster
  config.addResource(siteXml)
  config.reloadConfiguration
  log.debug(s"yarn config is $config")
  lazy val leader = LeaderCreator.getLeader(deploymentConfig)
  val containerListener = new NMCallbackHandler()
  val nodeManagerClient = new NMClientAsyncImpl(containerListener)
  val hdfs = new HdfsWrapper(true, deploymentConfig)
  val applicationId = args(0)
  val allocListener = new RMCallbackHandler(nodeManagerClient, deploymentConfig, applicationId, leader)
  val ressourcManagerClient: AMRMClientAsync[ContainerRequest] = AMRMClientAsync.createAMRMClientAsync(1000, allocListener)
  run

  def run() {
    try {
      initApplicationMaster
      leader.start
      startContainers
      waitAndStopApplicationMaster
    } finally {
      System.exit(0) // if there are still some threads running, they are killed by that (for example an ActorSystem)
    }
  }
  
  private def startLogServer{
    new SocketLogger().start()
  }

  /**
   * initalize clients to the ResourceManager and NodeManager
   */
  private def initApplicationMaster = {
    ressourcManagerClient.init(config)
    ressourcManagerClient.start()

    nodeManagerClient.init(config)
    nodeManagerClient.start()

    val appMasterHostname = NetUtils.getHostname();
    val response = ressourcManagerClient
      .registerApplicationMaster(appMasterHostname, -1, "")
  }

  /**
   * starts the container requested in the deploymentConfig
   */
  private def startContainers: Unit = {
    val containerAsk = setupContainerAskForRM()
    val numberOfNodes = deploymentConfig.numberOfNodes
    log.debug(s"requesting $numberOfNodes Containers")
    for (i <- 0 until numberOfNodes) {
      ressourcManagerClient.addContainerRequest(containerAsk)
    }
  }
 
  private def setupContainerAskForRM(): ContainerRequest = {
    val memory = deploymentConfig.memoryPerNode
    val memoryFactor = deploymentConfig.requestedMemoryFactor
    val pri = Records.newRecord(classOf[Priority])
    pri.setPriority(0)

    val capability = Records.newRecord(classOf[Resource])
    val memoryToRequest = (memory * memoryFactor)
    capability.setMemory(memoryToRequest.toInt)

    val request = new ContainerRequest(capability, null, null, pri)
    log.info("Requested container ask: " + request.toString())
    request
  }

  private def waitAndStopApplicationMaster: Unit = {
    waitFinish

    val appStatus = if (ContainerRegistry.successfull) FinalApplicationStatus.SUCCEEDED else FinalApplicationStatus.FAILED
    val appMessage = "finished"
    hdfs.deleteFolder(deploymentConfig.hdfsPath + "/" + applicationId + "/")
    try {
      ressourcManagerClient.unregisterApplicationMaster(appStatus, appMessage, null)
    } catch {
      case e: Exception => log.warn("failed unregister ApplicationMaster")
    }
    nodeManagerClient.stop()
    ressourcManagerClient.stop()

  }

  private def waitFinish: Unit = {
    val begin = System.currentTimeMillis()
    try {
      while ((!leader.isExecutionFinished) && timeoutNotReached(begin)) {
        Thread.sleep(100)
      }
      if (!timeoutNotReached(begin)) {
        log.warn("Timeout reached")
      }
      leader.shutdown
      while (!ContainerRegistry.isFinished) {
        Thread.sleep(100)
      }
    } catch {
      case e: Exception => {
        log.info("interrupted")
        throw e
      }
    }
  }

  def timeoutNotReached(begin: Long): Boolean = {
    val timeout = deploymentConfig.timeout
    (System.currentTimeMillis() - begin) / 1000 < timeout
  }

}

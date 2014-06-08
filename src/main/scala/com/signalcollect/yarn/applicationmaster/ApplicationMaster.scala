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
import java.io.File
import org.apache.hadoop.fs.Path
import com.signalcollect.util.ConfigProvider
import com.signalcollect.nodeprovisioning.yarn.LeaderCreator
import com.signalcollect.deployment.DeploymentConfigurationCreator
import com.signalcollect.deployment.yarn.YarnClientCreator
import com.signalcollect.deployment.yarn.DefaultYarnClientCreator

object ApplicationMaster extends App with LogHelper {
  YarnClientCreator.overrideFactory(new DefaultYarnClientCreator)
  val deploymentConfig = DeploymentConfigurationCreator.getDeploymentConfiguration
  val config: Configuration = YarnClientCreator.yarnClient.getConfig()
  val siteXml = new Path("dummy-yarn-site.xml") //this is needed for the minicluster
  config.addResource(siteXml)
  val containerListener = new NMCallbackHandler()
  val nodeManagerClient = new NMClientAsyncImpl(containerListener)

  val allocListener = new RMCallbackHandler(nodeManagerClient)
  val ressourcManagerClient: AMRMClientAsync[ContainerRequest] = AMRMClientAsync.createAMRMClientAsync(1000, allocListener)
  lazy val leader = LeaderCreator.getLeader(deploymentConfig)
  run

  def run() {
    initApplicationMaster
    leader.start
    startContainers
    waitAndStopApplicationMaster
  }

  private def initApplicationMaster = {
    ressourcManagerClient.init(config)
    ressourcManagerClient.start()

    nodeManagerClient.init(config)
    nodeManagerClient.start()

    val appMasterHostname = NetUtils.getHostname();
    val response = ressourcManagerClient
      .registerApplicationMaster(appMasterHostname, -1, "")
  }

  private def startContainers: Unit = {
    val containerAsk = setupContainerAskForRM()
    val numberOfNodes = deploymentConfig.numberOfNodes
    for (i <- 0 until numberOfNodes) {
      ressourcManagerClient.addContainerRequest(containerAsk)
    }
  }

  private def setupContainerAskForRM(): ContainerRequest = {
    val memory = ConfigProvider.config.getInt("deployment.containerMemory")
    val pri = Records.newRecord(classOf[Priority])
    pri.setPriority(0)

    val capability = Records.newRecord(classOf[Resource])
    capability.setMemory(memory)

    val request = new ContainerRequest(capability, null, null, pri)
    log.info("Requested container ask: " + request.toString())
    request
  }

  private def waitAndStopApplicationMaster: Unit = {
    waitFinish
    val appStatus = if (ContainerRegistry.successfull) FinalApplicationStatus.SUCCEEDED else FinalApplicationStatus.FAILED
    val appMessage = "finished"
    try {
      ressourcManagerClient.unregisterApplicationMaster(appStatus, appMessage, null)
    } catch {
      case e: Exception => log.info("failed unregister ApplicationMaster")
    }

    nodeManagerClient.stop()
    ressourcManagerClient.stop()
    System.exit(0) // if there are still some threads running, they are killed by that (for example an ActorSystem)
  }
  
  private def waitFinish: Unit = {
    try {
      while (!leader.isExecutionFinished) {
        Thread.sleep(100)
      }
    } catch {
      case e: Exception => log.info("interrupted")
    }
  }

}

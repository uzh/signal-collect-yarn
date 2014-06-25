/*
 *  @author Tobias Bachmann
 *
 *  Copyright 2012 University of Zurich
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

package com.signalcollect.deployment.amazon

import scala.collection.JavaConversions._
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig
import com.amazonaws.services.elasticmapreduce.model.ListInstancesRequest
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest
import com.amazonaws.services.elasticmapreduce.model.StepConfig
import com.amazonaws.services.elasticmapreduce.util.StepFactory
import com.signalcollect.deployment.Cluster
import com.signalcollect.deployment.DeploymentConfiguration
import com.signalcollect.deployment.ssh.SshTunnel
import com.signalcollect.deployment.ssh.TunnelConfiguration
import com.amazonaws.services.elasticmapreduce.model.ListInstanceGroupsRequest
import java.net.InetAddress
import java.net.Socket
import java.net.InetSocketAddress
import com.amazonaws.services.elasticmapreduce.model.TerminateJobFlowsRequest
import com.signalcollect.deployment.yarn.YarnCluster

class AmazonCluster extends Cluster {

  def deploy(deploymentConfiguration: DeploymentConfiguration): Boolean = {
    val amazonConfig = AmazonConfigurationCreator.getAmazonConfiguration
    val credentials = new BasicAWSCredentials(amazonConfig.accessKey, amazonConfig.secretKey)
    val emr = new AmazonElasticMapReduceClient(credentials)
    emr.setEndpoint(amazonConfig.endpoint)

    val clusterId = if (amazonConfig.clusterId == "") {
      createCluster(amazonConfig, emr)
    } else {
      amazonConfig.clusterId
    }
    val masterIp = getPublicAndPrivateIp(emr, clusterId)
    println(s"open tunnels to master on $masterIp to use cluster $clusterId")
    SshTunnel.open(new TunnelConfiguration(host = masterIp._1, remoteHost = masterIp._2))
//        while(true){
//          Thread.sleep(1000)
//        }
    val yarncluster = new YarnCluster
    yarncluster.setMasterIP(masterIp._2)
    val result = yarncluster.deploy(deploymentConfiguration)
    if (amazonConfig.stopCluster) {
      terminateCluster(emr, clusterId)
    }
    result
  }

  private def createCluster(amazonConfig: AmazonConfiguration, emr: AmazonElasticMapReduceClient): String = {
    val stepFactory = new StepFactory()

    val enabledebugging = new StepConfig()
      .withName("Enable debugging")
      .withActionOnFailure("TERMINATE_JOB_FLOW")
      .withHadoopJarStep(stepFactory.newEnableDebuggingStep())

    val request = new RunJobFlowRequest()
      .withName(amazonConfig.name)
      .withSteps(enabledebugging)
      .withLogUri(amazonConfig.s3Folder + "/logs")
      .withInstances(new JobFlowInstancesConfig()
        .withEc2KeyName(amazonConfig.keypair)
        .withHadoopVersion(amazonConfig.hadoopVersion)
        .withInstanceCount(amazonConfig.instanceCount)
        .withKeepJobFlowAliveWhenNoSteps(true)
        .withMasterInstanceType(amazonConfig.masterType)
        .withSlaveInstanceType(amazonConfig.slaveType))

    val result = emr.runJobFlow(request)
    val clusterId = result.getJobFlowId()
    waitClusterRunning(emr, clusterId)
    println(s"started cluster with id $clusterId")
    clusterId
  }

  def terminateCluster(emr: AmazonElasticMapReduceClient, clusterId: String) {
    val terminateRequest = new TerminateJobFlowsRequest().withJobFlowIds(clusterId)
    emr.terminateJobFlows(terminateRequest)
  }

  private def getPublicAndPrivateIp(emr: AmazonElasticMapReduceClient, clusterId: String): (String, String) = {
    val instanceRequest = new ListInstancesRequest()
    instanceRequest.setClusterId(clusterId)
    val instances = emr.listInstances(instanceRequest).getInstances()

    val ips = instances.map(ip => (ip.getPublicIpAddress, ip.getPrivateIpAddress())).toList
    instances.foreach(i =>println(i.getPrivateDnsName() + ": " + i.getPublicIpAddress()))
    val ip = SshTunnel.getOneOpenSsh(ips.map(ip => ip._1))
    ips.find(_._1 == ip).get
  }

  private def waitClusterRunning(emr: AmazonElasticMapReduceClient, clusterId: String): Unit = {
    while (!emr.listClusters().getClusters().filter(_.getId() == clusterId).forall(c => c.getStatus().getState() == "RUNNING" || c.getStatus().getState() == "WAITING")) {
      Thread.sleep(1000)
      emr.listClusters().getClusters().filter(_.getId() == clusterId).foreach(c => println(c.getStatus().getState()))
    }
  }

  def portIsOpen(ip: String, port: Int, timeout: Int): Boolean = {
    try {
      val socket = new Socket()
      socket.connect(new InetSocketAddress(ip, port), timeout)
      socket.close()
      true
    } catch {
      case e: Throwable => false
    }
  }
}

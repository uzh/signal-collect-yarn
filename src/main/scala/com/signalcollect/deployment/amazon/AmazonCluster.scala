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

import com.signalcollect.deployment.Cluster
import com.signalcollect.deployment.DeploymentConfiguration
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.elasticmapreduce.util.StepFactory
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig
import com.amazonaws.services.elasticmapreduce.model.StepConfig
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig

class AmazonCluster extends Cluster {

  def deploy(deploymentConfiguration: DeploymentConfiguration): Boolean = {
    val amazonConfig = AmazonConfigurationCreator.getAmazonConfiguration
    val credentials = new BasicAWSCredentials(amazonConfig.accessKey, amazonConfig.secretKey)
    val emr = new AmazonElasticMapReduceClient(credentials)
 
    val stepFactory = new StepFactory()

    val enabledebugging = new StepConfig()
      .withName("Enable debugging")
      .withActionOnFailure("TERMINATE_JOB_FLOW")
      .withHadoopJarStep(stepFactory.newEnableDebuggingStep())

    val hadoopConfig1 = new HadoopJarStepConfig()
      .withJar("./target/")
      .withMainClass("com.signalcollect.yarn.applicationmaster.ApplicationMaster")
      .withArgs("applicationId")
    val customStep = new StepConfig("Step1", hadoopConfig1)

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
    true
  }

}




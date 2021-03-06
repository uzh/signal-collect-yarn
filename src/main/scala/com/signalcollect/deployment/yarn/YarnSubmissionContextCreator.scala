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
package com.signalcollect.deployment.yarn

import com.typesafe.config.Config
import com.signalcollect.logging.Logging
import java.util.HashMap
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.{ YarnClient, YarnClientApplication }
import org.apache.hadoop.yarn.util.Records
import scala.collection.JavaConversions._

/**
 * Creates a SubmissionContext.
 */
class YarnSubmissionContextCreator(client: YarnClient, application: YarnClientApplication, launchSettings: LaunchSettings, deploymentConf: YarnDeploymentConfiguration) {
  private lazy val submissionContext = application.getApplicationSubmissionContext()
  private lazy val applicationId = submissionContext.getApplicationId().toString()
  private lazy val launchContextFactory = new YarnContainerLaunchContextCreator(
    launchSettings =launchSettings.copy(mainClass = deploymentConf.applicationMaster,
      arguments = applicationId :: launchSettings.arguments),
      deployConfig = deploymentConf)
  private lazy val launchContext: ContainerLaunchContext =
    launchContextFactory.createLaunchContext(applicationId)
  val memory = launchSettings.memory
  val memoryFactor = deploymentConf.requestedMemoryFactor 

  def getSubmissionContext(): ApplicationSubmissionContext = {
    setupLaunchAndSubmissionContext(submissionContext)
    submissionContext
  }

  def setupLaunchAndSubmissionContext(submissionContext: ApplicationSubmissionContext): ApplicationSubmissionContext = {
    setupLaunchContext(submissionContext)
    submissionContext.setApplicationName(deploymentConf.applicationName)
    val capability = Records.newRecord(classOf[Resource])
    val requestMemory = memory * memoryFactor
    capability.setMemory(requestMemory.toInt)
    submissionContext.setResource(capability)
    submissionContext
  }

  private def setupLaunchContext(submissionContext: ApplicationSubmissionContext): Unit = {

    submissionContext.setAMContainerSpec(launchContext)
  }

}
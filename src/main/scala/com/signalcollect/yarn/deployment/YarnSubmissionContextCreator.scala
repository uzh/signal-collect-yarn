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
package com.signalcollect.yarn.deployment

import com.typesafe.config.Config
import com.signalcollect.util.LogHelper
import java.util.HashMap
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.{YarnClient, YarnClientApplication}
import org.apache.hadoop.yarn.util.Records
import scala.collection.JavaConversions._
import com.signalcollect.util.ConfigProvider

class YarnSubmissionContextCreator(client: YarnClient, application: YarnClientApplication, launchSettings: LaunchSettings) {
  private val config = ConfigProvider.config
  private lazy val submissionContext = application.getApplicationSubmissionContext()
  private lazy val applicationId = submissionContext.getApplicationId().toString()
  private lazy val launchContextFactory = new YarnContainerLaunchContextCreator(
      new LaunchSettings(mainClass = config.getString("deployment.applicationMaster"), pathsToJars = launchSettings.pathsToJars))
  private lazy val launchContext: ContainerLaunchContext =
    launchContextFactory.createLaunchContext(applicationId)
  val memory = config.getInt("deployment.memory")
  
  def getSubmissionContext(): ApplicationSubmissionContext = {
    setupLaunchAndSubmissionContext(submissionContext)
    submissionContext
  }

  def setupLaunchAndSubmissionContext(submissionContext: ApplicationSubmissionContext): ApplicationSubmissionContext = {
    setupLaunchContext(submissionContext)
    submissionContext.setApplicationName(config.getString("deployment.applicationName"))
    val capability = Records.newRecord(classOf[Resource])
    capability.setMemory(memory)
    submissionContext.setResource(capability)
    submissionContext
  }

  private def setupLaunchContext(submissionContext: ApplicationSubmissionContext): Unit = {

    submissionContext.setAMContainerSpec(launchContext)
  }
  
}
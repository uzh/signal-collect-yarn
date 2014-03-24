/*
 *  @author Tobias Bachmann
 *
 *  Copyright 2013 University of Zurich
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
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext
import com.signalcollect.util.ConfigProvider
import org.apache.hadoop.yarn.api.records.ApplicationId

class YarnDeploymentClient(launchSettings: LaunchSettings) {
  val config = ConfigProvider.config
  lazy val yarnClient = YarnClientCreator.yarnClient
  lazy val application = YarnApplicationCreator.getApplication(config, yarnClient)
  lazy val submissionContext = createSubmissionContext()

  def createSubmissionContext(): ApplicationSubmissionContext = {
    val submissionFactory = new YarnSubmissionContextCreator(yarnClient, application, launchSettings)
    submissionFactory.getSubmissionContext
  }

  def submitApplication(): ApplicationId = {
    yarnClient.submitApplication(submissionContext)
  }
}
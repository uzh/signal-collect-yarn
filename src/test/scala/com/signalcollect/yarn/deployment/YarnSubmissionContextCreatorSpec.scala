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

import org.junit.runner.RunWith
import org.specs2.mutable.SpecificationWithJUnit
import com.signalcollect.util.ConfigProvider
import com.signalcollect.yarn.applicationmaster.ApplicationMaster
import org.specs2.runner.JUnitRunner
import com.signalcollect.deployment.DeploymentConfigurationCreator

@RunWith(classOf[JUnitRunner])
class YarnSubmissionContextFactorySpec extends SpecificationWithJUnit {
  "YarnSubmissionContextCreator" should {
    println("Test executing now: YarnSubmissionContextCreator")
    val config = ConfigProvider.config
    lazy val yarnClient = YarnClientCreator.yarnClient
    lazy val application = YarnApplicationCreator.getApplication(config, yarnClient)
    "call Factory" in {
      val launchSettings = LaunchSettingsCreator.getSettingsForClass(ApplicationMaster.getClass(), DeploymentConfigurationCreator.getDeploymentConfiguration)
      val factory = new YarnSubmissionContextCreator(yarnClient, application, launchSettings)
      val context =factory.getSubmissionContext()
      context.getApplicationName() === config.getString("deployment.applicationName")
    }
  }
}
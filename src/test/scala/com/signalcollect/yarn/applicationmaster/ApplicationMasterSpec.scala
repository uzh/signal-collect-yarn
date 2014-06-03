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

import com.signalcollect.util.LogHelper
import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.specs2.mutable.SpecificationWithJUnit
import com.signalcollect.yarn.deployment.YarnClientCreator
import com.signalcollect.yarn.deployment.YarnDeploymentClient
import scala.collection.JavaConversions._
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus
import com.signalcollect.yarn.deployment.LaunchSettings
import com.signalcollect.yarn.deployment.MiniCluster
import java.nio.file.Path
import java.io.File
import com.signalcollect.yarn.deployment.LaunchSettingsCreator
import com.signalcollect.util.ConfigProvider

@RunWith(classOf[JUnitRunner])
class ApplicationMasterSpec extends SpecificationWithJUnit {

  "ApplicationMaster" should {

    "run application successfull" in {
      println("Test executing now: ApplicationMasterSpec")
      val typesafeConfig = ConfigProvider.config 
      val yarnClient = YarnClientCreator.yarnClient
      val launchSettings = LaunchSettingsCreator.getSettingsForClass(ApplicationMaster.getClass())
      val client = new YarnDeploymentClient(launchSettings)
      val application = client.submitApplication()
      var finished = false
      while (!finished) {
        Thread.sleep(1000)
        val apps = yarnClient.getApplications.toList
        if (apps.size() == 0) {
          Thread.sleep(10)
        } else {
          if (apps.exists(_.getApplicationId().equals(application))) {
            val appReport = apps.find(_.getApplicationId().equals(application)).get
            val applicationState = appReport.getYarnApplicationState()
            println("ApplicationState = " + applicationState)
            if (applicationState == YarnApplicationState.FINISHED || applicationState == YarnApplicationState.FAILED) {
              finished = true
            }
          }
        }
      }
      val appReport = yarnClient.getApplications.toList.find(_.getApplicationId().equals(application)).get
      appReport.getFinalApplicationStatus() === FinalApplicationStatus.SUCCEEDED
    }
  }
}
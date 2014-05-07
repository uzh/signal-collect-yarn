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
import com.typesafe.config.ConfigParseOptions
import com.typesafe.config.ConfigResolveOptions
import java.net.UnknownHostException
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.matcher.Matcher
import com.typesafe.config.ConfigFactory
import com.signalcollect.util.ConfigProvider
import com.signalcollect.yarn.applicationmaster.ApplicationMaster


@RunWith(classOf[JUnitRunner])
class YarnDeploymentClientSpec extends SpecificationWithJUnit {
//  "YarnClient" should {
//    sequential
//    val typesafeConfig = ConfigProvider.config
//    val launchSettings = TestLaunchSettings.getSettingsForClass(ApplicationMaster.getClass())
//    val client = new YarnDeploymentClient(launchSettings)
//
//    "Yarn Client should get ClusterMetrics" in {
//      val yarnClient = client.yarnClient
//      val clusterMetrics = yarnClient.getYarnClusterMetrics()
//      clusterMetrics !== null
//    }
//    
//    "create submission context" in {
//      client.submissionContext.getApplicationName() === typesafeConfig.getString("deployment.applicationName")
//    }
//    
//    "submit Application" in {
//      client.submitApplication() must not(throwA[Exception])
//    }
//    
//  }

}
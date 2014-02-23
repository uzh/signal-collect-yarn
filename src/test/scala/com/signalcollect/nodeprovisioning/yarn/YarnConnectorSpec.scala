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
package com.signalcollect.nodeprovisioning.yarn

import com.typesafe.config.Config
import com.typesafe.config.ConfigParseOptions
import com.typesafe.config.ConfigResolveOptions
import java.net.UnknownHostException
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.matcher.Matcher
import com.typesafe.config.ConfigFactory

@RunWith(classOf[JUnitRunner])
class YarnConnectorSpec extends SpecificationWithJUnit {
  "YarnConnector" should {

    val typesafeConfig = ConfigFactory.load("test-deployment")
    val connector = new YarnConnector(typesafeConfig)

    "create a new YarnConfiguration which contains RM address" in {
      val yarnConfig = connector.yarnConfig
      val yarnHost = "yarn.resourcemanager.host"
      val yarnAddress = "yarn.resourcemanager.address"
      val deploymentHost = typesafeConfig.getString("deployment." + yarnHost)
      val deploymentAddress = typesafeConfig.getString("deployment." + yarnAddress)
      yarnConfig.get(yarnHost) must be(deploymentHost)
      yarnConfig.get(yarnAddress) must be(deploymentAddress)
    }

    "Yarn Client should get ClusterMetrics" in {
      val yarnClient = connector.yarnClient
      val clusterMetrics = yarnClient.getYarnClusterMetrics()
      clusterMetrics must not be_== (null)
    }
    
    "throws Exception when RM is not known" in {
      val badConfig = ConfigFactory.load("bad-test-deployment")
      val badConnector = new YarnConnector(badConfig)
      badConnector.application must throwA[UnknownHostException]
    }
    
    "when an app is created a GetNewApplicationResponse should be available" in {
      val app = connector.application
      val response = connector.response
      response.getApplicationId() !== null
    }
    
    "create submission context" in {
      connector.submissionContext.getApplicationName() === typesafeConfig.getString("deployment.applicationName")
    }
    
    "create launch context" in {
      connector.launchContext !== null
    }
    
  }

}
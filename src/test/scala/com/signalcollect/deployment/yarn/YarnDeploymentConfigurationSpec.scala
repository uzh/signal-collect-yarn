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
 */

package com.signalcollect.deployment.yarn

import org.scalatest.prop.Checkers
import org.scalatest.FlatSpec
import com.typesafe.config.ConfigFactory

class YarnConfigurationSpec extends FlatSpec with Checkers {

  def createYarnDeploymentConfiguration: YarnDeploymentConfiguration = {
    val configAsString =
      """deployment{
           memory-per-node = 512
	       jvm-arguments = "-XX:+AggressiveOpts"
	       number-of-nodes = 1
	       copy-files = ["some/file"]
	       algorithm = "com.signalcollect.deployment.PageRankExample"
	       algorithm-parameters {
		     "parameter-name" = "some-parameter"
	       }
	       cluster = "com.signalcollect.deployment.TestCluster"
    	   application-name = "signal-collect-yarn-deployment"
    	}"""
    val config = ConfigFactory.parseString(configAsString)
    YarnDeploymentConfigurationCreator.getYarnDeploymentConfiguration(config)
  }

  "AmazonConfiguration" should "contain applicationName" in {
    val deploymentConfig = createYarnDeploymentConfiguration
    assert(deploymentConfig.applicationName === "signal-collect-yarn-deployment")
  }
}
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

import org.junit.runner.RunWith
import org.specs2.mutable.SpecificationWithJUnit
import com.signalcollect.deployment.DeploymentConfiguration
import com.signalcollect.deployment.DeploymentConfigurationCreator
import com.signalcollect.deployment.yarn.YarnCluster
import com.typesafe.config.ConfigFactory
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ApplicationMasterSpec extends SpecificationWithJUnit {
  
 def createDeploymentConfiguration(cluster: String): DeploymentConfiguration = {
    val configAsString =
      s"""deployment {
	       memory-per-node = 512
	       jvm-arguments = ""
	       number-of-nodes = 1
	       copy-files = []
	       algorithm = "com.signalcollect.deployment.PageRankExample"
	       algorithm-parameters {
		     "parameter-name" = "some-parameter"
	       }
	       cluster = "${cluster}"
         }"""
    val config = ConfigFactory.parseString(configAsString)
    DeploymentConfigurationCreator.getDeploymentConfiguration(config)
  }
     
  "ApplicationMaster" should {

    "run application successfull" in {
      val deploymentConf = createDeploymentConfiguration("com.signalcollect.deployment.yarn.YarnCluster")
      println(deploymentConf.algorithm)
      val cluster = new YarnCluster()
      cluster.testDeployment = true
      cluster.deploy(deploymentConf) === true
    }
  }
}
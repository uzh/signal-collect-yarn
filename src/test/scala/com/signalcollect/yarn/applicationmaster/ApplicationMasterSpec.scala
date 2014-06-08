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
import com.signalcollect.deployment.ClusterCreator
import com.signalcollect.deployment.DeploymentConfigurationCreator
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ApplicationMasterSpec extends SpecificationWithJUnit {

  "ApplicationMaster" should {

    "run application successfull" in {
      val deploymentConf = DeploymentConfigurationCreator.getDeploymentConfiguration
      val cluster = ClusterCreator.getCluster(deploymentConf)
      cluster.deploy(deploymentConf) === true
    }
  }
}
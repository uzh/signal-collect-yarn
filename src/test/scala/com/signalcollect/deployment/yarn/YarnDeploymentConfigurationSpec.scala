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
import com.typesafe.config.Config

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
           leader-memory = 512
           application-master = "com.signalcollect.yarn.applicationmaster.ApplicationMaster"
           requested-memory-factor = 1.1
           container-class = "com.signalcollect.deployment.NodeContainerApp"
           path-to-jar = "target/scala-2.11/signal-collect-yarn-assembly-1.0-SNAPSHOT.jar"
           files-on-hdfs = ["testFile"]
           hdfspath = "signal-collect-yarn-deployment"
           timeout = 400
           user = "tbachmann"
           akka {
	         port: 2552
             kryo-initializer = "com.signalcollect.configuration.KryoInit"
             kryo-registrations = [
               #"some.class.to.be.registered"
             ]
             serialize-messages = true
	       }
           hadoop-overrides {
  	         yarn {
  		       resourcemanager {
  			     host = "127.0.0.1"
  			  
  		        }
  	          }
	       }
    	}
        testing{
          useMiniCluster = true
          createJarOnTheFly = true
          dependency = "../signal-collect-yarn-dependencies/target/scala-2.11/signal-collect-yarn-assembly-1.0-SNAPSHOT.jar:../signal-collect/target/scala-2.11/signal-collect-2.1-SNAPSHOT.jar"
          onHdfs = false
       }"""
    val config = ConfigFactory.parseString(configAsString)
    YarnDeploymentConfigurationCreator.getYarnDeploymentConfiguration(config)
  }

  "YarnConfiguration" should "contain applicationName" in {
    val deploymentConfig = createYarnDeploymentConfiguration
    assert(deploymentConfig.applicationName === "signal-collect-yarn-deployment")
  }

  it should "contain leaderMemory" in {
    val deploymentConfig = createYarnDeploymentConfiguration
    assert(deploymentConfig.leaderMemory === 512)
  }

  it should "contain applicationMaster" in {
    val deploymentConfig = createYarnDeploymentConfiguration
    assert(deploymentConfig.applicationMaster === "com.signalcollect.yarn.applicationmaster.ApplicationMaster")
  }

  it should "contain requestedMemoryFactor" in {
    val deploymentConfig = createYarnDeploymentConfiguration
    assert(deploymentConfig.requestedMemoryFactor === 1.1)
  }

  it should "contain container-class" in {
    val deploymentConfig = createYarnDeploymentConfiguration
    assert(deploymentConfig.containerClass === "com.signalcollect.deployment.NodeContainerApp")
  }

  it should "contain pathToJar" in {
    val deploymentConfig = createYarnDeploymentConfiguration
    assert(deploymentConfig.pathToJar === "target/scala-2.11/signal-collect-yarn-assembly-1.0-SNAPSHOT.jar")
  }

  it should "contain filesOnHdfs" in {
    val deploymentConfig = createYarnDeploymentConfiguration
    assert(deploymentConfig.filesOnHdfs === List("testFile"))
  }

  it should "contain hdfsPath" in {
    val deploymentConfig = createYarnDeploymentConfiguration
    assert(deploymentConfig.hdfsPath === "signal-collect-yarn-deployment")
  }

  it should "contain timeout" in {
    val deploymentConfig = createYarnDeploymentConfiguration
    assert(deploymentConfig.timeout === 400)
  }

  it should "contain user" in {
    val deploymentConfig = createYarnDeploymentConfiguration
    assert(deploymentConfig.user === "tbachmann")
  }
  
  it should "contain hadoop overrides" in {
	  val deploymentConfig = createYarnDeploymentConfiguration
	  assert(deploymentConfig.hadoopOverrides.getString("yarn.resourcemanager.host") === "127.0.0.1")
  }
  
  it should "contain testing" in {
	  val deploymentConfig = createYarnDeploymentConfiguration
	  assert(deploymentConfig.useMiniCluster === true)
	  assert(deploymentConfig.createJarOnTheFly === true)
	  assert(deploymentConfig.testDependencies === "../signal-collect-yarn-dependencies/target/scala-2.11/signal-collect-yarn-assembly-1.0-SNAPSHOT.jar:../signal-collect/target/scala-2.11/signal-collect-2.1-SNAPSHOT.jar")
	  assert(deploymentConfig.testDependenciesOnHdfs === false)
  }
}
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

package com.signalcollect.deployment.amazon

import org.scalatest.prop.Checkers
import org.scalatest.FlatSpec
import com.typesafe.config.ConfigFactory

class AmazonConfigurationSpec extends FlatSpec with Checkers {

  def createAmazonConfiguration: AmazonConfiguration = {
    val configAsString =
      """amazon{
    		name = "signalcollect"
    		access-key = "access"
    		secret-key = "secret"
    		s3-folder = "s3://myawsbucket/"
    		ec2-keypair = "signalcollect"
    		instance-count = 2
    		master-type = "m1.small" 
    		slave-type = "m1.small"
    		hadoop-version = "2.2.0"
            endpoint = "elasticmapreduce.eu-west-1.amazonaws.com"
            clusterId = "clusterId"
    	}"""
    val config = ConfigFactory.parseString(configAsString)
    AmazonConfigurationCreator.getAmazonConfiguration(config)
  }

  "AmazonConfiguration" should "contain name" in {
    val deploymentConfig = createAmazonConfiguration
    assert(deploymentConfig.name === "signalcollect")
  }

  it should "contain accessKey" in {
    val deploymentConfig = createAmazonConfiguration
    assert(deploymentConfig.accessKey === "access")
  }

  it should "contain secretKey" in {
    val deploymentConfig = createAmazonConfiguration
    assert(deploymentConfig.secretKey === "secret")
  }

  it should "contain s3Folder" in {
    val deploymentConfig = createAmazonConfiguration
    assert(deploymentConfig.s3Folder === "s3://myawsbucket/")
  }

  it should "contain keypair" in {
    val deploymentConfig = createAmazonConfiguration
    assert(deploymentConfig.keypair === "signalcollect")
  }

  it should "contain instanceCount" in {
    val deploymentConfig = createAmazonConfiguration
    assert(deploymentConfig.instanceCount === 2)
  }

  it should "contain masterType" in {
    val deploymentConfig = createAmazonConfiguration
    assert(deploymentConfig.masterType === "m1.small")
  }

  it should "contain slaveType" in {
    val deploymentConfig = createAmazonConfiguration
    assert(deploymentConfig.slaveType === "m1.small")
  }
  
  it should "contain hadoopVersion" in {
    val deploymentConfig = createAmazonConfiguration
    assert(deploymentConfig.hadoopVersion === "2.2.0")
  }
  
  it should "contain endpoint" in {
    val deploymentConfig = createAmazonConfiguration
    assert(deploymentConfig.endpoint === "elasticmapreduce.eu-west-1.amazonaws.com")
  }
  
  it should "contain clusterId" in {
	  val deploymentConfig = createAmazonConfiguration
			  assert(deploymentConfig.clusterId === "clusterId")
  }
}
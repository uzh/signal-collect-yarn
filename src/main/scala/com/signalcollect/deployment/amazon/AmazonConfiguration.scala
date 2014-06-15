/*
 *  @author Tobias Bachmann
 *
 *  Copyright 2011 University of Zurich
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

package com.signalcollect.deployment.amazon

import com.typesafe.config.ConfigFactory
import java.io.File
import scala.collection.JavaConversions._
import com.typesafe.config.Config

/**
 * All the amazon parameters
 */
case class AmazonConfiguration(
  name: String = "signalcollect",
  accessKey: String = "changeme",
  secretKey: String = "changeme",
  s3Folder: String = "s3://myawsbucket/",
  keypair: String = "keypair",
  instanceCount: Int = 2,
  masterType: String = "m1.small",
  slaveType: String = "m1.small",
  hadoopVersion: String = "2.2.0",
  endpoint: String = "elasticmapreduce.eu-west-1.amazonaws.com",
  clusterId: String = "")

/**
 * Creator of AmazonConfiguration reads configuration from file 'amazon.conf'
 */
object AmazonConfigurationCreator {
  val amazon = ConfigFactory.parseFile(new File("amazon.conf"))

  /**
   * creates DeploymentConfiguration out of 'amazon.conf'
   */
  def getAmazonConfiguration: AmazonConfiguration = getAmazonConfiguration(amazon)

  /**
   * can be called with another Config, useful for testing or injecting another configuration than 'amazon.conf'
   */
  def getAmazonConfiguration(config: Config): AmazonConfiguration =
    new AmazonConfiguration(
      name = config.getString("amazon.name"),
      accessKey = config.getString("amazon.access-key"),
      secretKey = config.getString("amazon.secret-key"),
      s3Folder = config.getString("amazon.s3-folder"),
      keypair = config.getString("amazon.ec2-keypair"),
      instanceCount = config.getInt("amazon.instance-count"),
      masterType = config.getString("amazon.master-type"),
      slaveType = config.getString("amazon.slave-type"),
      hadoopVersion = config.getString("amazon.hadoop-version"),
      endpoint = config.getString("amazon.endpoint"),
      clusterId = config.getString("amazon.clusterId"))

  /**
   * useful for testing or injecting another configuration than 'amazon.conf'
   */
  def getAmazonConfiguration(configPath: String): AmazonConfiguration = {
    val config = ConfigFactory.parseFile(new File(configPath))
    getAmazonConfiguration(config)
  }

}
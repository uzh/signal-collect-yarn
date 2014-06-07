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
package com.signalcollect.util

import com.typesafe.config.ConfigFactory
import java.io.File
import com.signalcollect.deployment.DeploymentConfiguration
import scala.collection.JavaConversions._

object ConfigProvider {
  //change name of config you use here
  val deployment = ConfigFactory.parseFile(new File("deployment.conf"))
  val yarn = ConfigFactory.parseFile(new File("yarn.conf"))
  val testing = ConfigFactory.parseFile(new File("yarn-testing.conf"))
  val config = testing.withFallback(yarn).withFallback(deployment)
  
  def getDeploymentConfiguration: DeploymentConfiguration = 
    new DeploymentConfiguration(
  algorithm = deployment.getString("deployment.algorithm"),
  algorithmParameters= getAlgorithmParameters,
  memoryPerNode= deployment.getInt("deployment.memory-per-node"),
  numberOfNodes= deployment.getInt("deployment.number-of-nodes"),
  copyFiles= deployment.getStringList("deployment.copy-files").toList, // list of paths to files
  clusterType = deployment.getString("deployment.type"))
  
  def getAlgorithmParameters: Map[String,String] = {
    deployment.getConfig("deployment.algorithm-parameters").entrySet.map {
      entry => (entry.getKey, entry.getValue.unwrapped.toString)
    }.toMap
  }
}
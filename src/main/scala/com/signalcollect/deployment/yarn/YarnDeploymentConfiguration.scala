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

package com.signalcollect.deployment.yarn

import com.typesafe.config.ConfigFactory
import java.io.File
import scala.collection.JavaConversions._
import com.typesafe.config.Config
import com.signalcollect.deployment.amazon.AmazonConfiguration
import com.signalcollect.deployment.DeploymentConfiguration
import com.signalcollect.deployment.DeploymentConfigurationCreator

/**
 * extends the basic DeploymentConfiguration and adds Yarn specifc values
 */
case class YarnDeploymentConfiguration(
  override val algorithm: String, //class name of a DeployableAlgorithm
  override val algorithmParameters: Map[String, String],
  override val memoryPerNode: Int = 512,
  override val numberOfNodes: Int = 1,
  override val copyFiles: List[String] = Nil, // list of paths to files
  override val cluster: String = "com.signalcollect.deployment.LeaderCluster",
  override val jvmArguments: String = "",
  applicationName: String = "signal-collect-yarn-deployment") extends DeploymentConfiguration

/**
 * Creator of YarnConfiguration reads configuration from file 'deployment.conf'
 */
object YarnDeploymentConfigurationCreator {
  val yarnConfig = ConfigFactory.parseFile(new File("deployment.conf"))

  /**
   * creates DeploymentConfiguration out of 'amazon.conf'
   */
  def getYarnDeploymentConfiguration: YarnDeploymentConfiguration = getYarnDeploymentConfiguration(yarnConfig)
  
  def getYarnDeploymentConfiguration(config: Config): YarnDeploymentConfiguration = {
    val basicConfig = DeploymentConfigurationCreator.getDeploymentConfiguration(config)
    getYarnDeploymentConfiguration(config, basicConfig)
  }
  
  def getYarnDeploymentConfiguration(basicConfig: DeploymentConfiguration): YarnDeploymentConfiguration = {
    getYarnDeploymentConfiguration(yarnConfig, basicConfig)
  }

  /**
   * can be called with another Config, useful for testing or injecting another configuration than 'deployment.conf', merges in a basic configuration
   */
  def getYarnDeploymentConfiguration(config: Config, basicConfig: DeploymentConfiguration): YarnDeploymentConfiguration = {

    /**
     * gets an object of Type T from the Config, when key not exists it returns None
     */
    def get[T](path: String): Option[T] = {
      if (config.hasPath(path))
        Some(config.getAnyRef(path).asInstanceOf[T])
      else None
    }
    new YarnDeploymentConfiguration(
      algorithm = basicConfig.algorithm,
      algorithmParameters = basicConfig.algorithmParameters,
      memoryPerNode = basicConfig.memoryPerNode,
      numberOfNodes = basicConfig.numberOfNodes,
      copyFiles = basicConfig.copyFiles,
      cluster = basicConfig.cluster,
      jvmArguments = basicConfig.jvmArguments,
      applicationName = get[String]("deployment.application-name").getOrElse("signal-collect-yarn-deployment"))
  }

  /**
   * useful for testing or injecting another configuration than 'deployment.conf'
   */
  def getYarnDeploymentConfiguration(configPath: String): YarnDeploymentConfiguration = {
    val config = ConfigFactory.parseFile(new File(configPath))
    getYarnDeploymentConfiguration(config)
  }

}
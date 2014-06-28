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
  applicationName: String = "signal-collect-yarn-deployment",
  leaderMemory: Int = 512,
  applicationMaster: String = "com.signalcollect.yarn.applicationmaster.ApplicationMaster",
  requestedMemoryFactor: Double = 1.1,
  containerClass: String = "com.signalcollect.deployment.ContainerNodeApp",
  pathToJar: String = "target/scala-2.11/signal-collect-yarn-assembly-1.0-SNAPSHOT.jar",
  filesOnHdfs: List[String] = Nil,
  hdfsPath: String = "",
  timeout: Int = 1,
  user: String = "hadoop") extends DeploymentConfiguration

/**
 * Creator of YarnConfiguration reads configuration from file 'deployment.conf'
 */
object YarnDeploymentConfigurationCreator {
  val yarn = ConfigFactory.parseFile(new File("deployment.conf"))
  val testing = ConfigFactory.parseFile(new File("yarn-testing.conf"))
  val testDeployment = ConfigFactory.parseFile(new File("testdeployment.conf"))
  val yarnConfig = testing.withFallback(yarn).withFallback(testDeployment)

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
    
    def getList[T](path: String): Option[List[T]] = {
      if (config.hasPath(path))
        Some(config.getAnyRefList(path).toList.map(e => e.asInstanceOf[T]))
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
      applicationName = get[String]("deployment.application-name").getOrElse("signal-collect-yarn-deployment"),
      leaderMemory = get[Int]("deployment.leader-memory").getOrElse(512),
      applicationMaster = get[String]("deployment.application-master").getOrElse("com.signalcollect.yarn.applicationmaster.ApplicationMaster"),
      requestedMemoryFactor = get[Double]("deployment.requested-memory-factor").getOrElse(1.1),
      containerClass = get[String]("deployment.container-class").getOrElse("com.signalcollect.deployment.ContainerNodeApp"),
      pathToJar = get[String]("deployment.path-to-jar").getOrElse("target/scala-2.11/signal-collect-yarn-assembly-1.0-SNAPSHOT.jar"),
      filesOnHdfs= getList[String]("deployment.files-on-hdfs").getOrElse(Nil),
      hdfsPath = get[String]("deployment.hdfspath").getOrElse("~"),
      timeout = get[Int]("deployment.timeout").getOrElse(1),
      user = get[String]("deployment.user").getOrElse("hadoop"))
  }

  /**
   * useful for testing or injecting another configuration than 'deployment.conf'
   */
  def getYarnDeploymentConfiguration(configPath: String): YarnDeploymentConfiguration = {
    val config = ConfigFactory.parseFile(new File(configPath))
    getYarnDeploymentConfiguration(config)
  }

}
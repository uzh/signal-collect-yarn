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
package com.signalcollect.deployment.yarn

import com.typesafe.config.Config
import com.signalcollect.util.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.client.api.YarnClient
import scala.collection.JavaConversions._

class DefaultYarnClientCreator(masterIp: String = "localhost", deploymentConfig: YarnDeploymentConfiguration) extends YarnClientCreatorImpl with Logging {
  override lazy val yarnClient = createYarnClient

  def createYarnClient: YarnClient = {
    val yarnConfig = new YarnConfiguration()
    val yarnOverrides = deploymentConfig.hadoopOverrides.entrySet.iterator
    val tupleListOverrides = yarnOverrides.map(entry => (entry.getKey, entry.getValue.unwrapped.toString)).toList
    val replaceMaster = tupleListOverrides.map(entry => (entry._1, entry._2.replaceAll("master", masterIp)))
    yarnConfig.set("fs.hdfs.impl",
      classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    yarnConfig.set("fs.file.impl",
      classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
    replaceMaster.foreach(e => yarnConfig.set(e._1, e._2))
    yarnConfig.reloadConfiguration
    createYarnClient(yarnConfig)
  }

  protected def createYarnClient(config: Configuration): YarnClient = {
    val yarnClient = YarnClient.createYarnClient()
    yarnClient.init(config)
    yarnClient.start()
    yarnClient
  }

}
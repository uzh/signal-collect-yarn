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
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.MiniYARNCluster
import org.apache.hadoop.conf.Configuration

class MiniYarnClientCreator extends YarnClientCreatorImpl {
  lazy val configuration = MiniCluster.getClusterConfig()
  override def yarnClient(): YarnClient = {
    val client = YarnClient.createYarnClient()
    val conf = new Configuration(configuration)
    client.init(conf)
    client.start()
    client
  }

  def stopMiniCluster() = {
      MiniCluster.cluster.stop()
  }
}
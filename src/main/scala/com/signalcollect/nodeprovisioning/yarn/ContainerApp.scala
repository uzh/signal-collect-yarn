/**
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

package com.signalcollect.nodeprovisioning.yarn

import com.signalcollect.util.ConfigProvider
import com.signalcollect.util.LogHelper
import java.net.InetAddress

object ContainerApp extends App with LogHelper {
  val basePort = ConfigProvider.config.getInt("deployment.akka.port")
  val nodeId = args(0).toInt
  val ip = InetAddress.getLocalHost.getHostAddress()
  log.info("start DefaultActor on node " + nodeId + " with ip " + ip)
  val numberOfNodes = ConfigProvider.config.getInt("deployment.numberOfNodes")
  val nodeBootstrap = new YarnNodeBootstrap(nodeId, numberOfNodes, basePort)
  val actorRef = nodeBootstrap.startNode
  println("started")
  Thread.sleep(20000) // need to detect when execution is terminated
  log.info("stop Node")
  nodeBootstrap.stopNode
  System.exit(0)
}
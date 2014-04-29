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
package com.signalcollect.nodeprovisioning.yarn

import akka.actor.ActorSystem
import com.signalcollect.configuration.ActorSystemRegistry
import com.signalcollect.configuration.AkkaConfig
import akka.event.Logging
import akka.actor.ActorRef
import com.signalcollect.util.ConfigProvider
import scala.collection.JavaConversions._
import com.signalcollect.deployment.YarnDeployableAlgorithm
import com.signalcollect.util.LogHelper
import com.signalcollect.nodeprovisioning.AkkaHelper

class Leader(nodes: List[ContainerInfo], akkaPort: Int, kryoRegistrations: List[String], kryoInit: String = "com.signalcollect.configuration.KryoInit") extends LogHelper {
  if (nodes.isEmpty) throw new IllegalArgumentException("There should be at least one node")

  val system: ActorSystem = ActorSystemRegistry.retrieve("SignalCollect").getOrElse(startActorSystem)

  def akkaConfig(akkaPort: Int, kryoRegistrations: List[String]) = AkkaConfig.get(
    akkaMessageCompression = true,
    serializeMessages = true,
    loggingLevel = Logging.WarningLevel, //Logging.DebugLevel,Logging.WarningLevel
    kryoRegistrations = kryoRegistrations,
    kryoInitializer = kryoInit,
    port = akkaPort)

  def startActorSystem: ActorSystem = {
    try {
      val system = ActorSystem("SignalCollect", akkaConfig(akkaPort, kryoRegistrations))
      ActorSystemRegistry.register(system)
      system
    } catch {
      case e: Exception => {
        log.info("failed to start actor system: " + e.getMessage())
        throw e
      }
    }
  }

  def getActorRefs: List[ActorRef] = {
    val nodeActors = nodes.map(node => system.actorFor(node.actorAddress))
    val one: Int = 1
    nodeActors.foreach(n => n ! one)
    nodeActors
  }

  def startExecution {
    val algorithm = ConfigProvider.config.getString("deployment.algorithm.class")
    val parameters = ConfigProvider.config.getConfig("deployment.algorithm.parameters").entrySet.map {
      entry => (entry.getKey, entry.getValue.unwrapped.toString)
    }.toMap
    try {
      val nodeActors = getActorRefs.toArray
      val algorithmObject = Class.forName(algorithm).newInstance.asInstanceOf[YarnDeployableAlgorithm]
      algorithmObject.execute(parameters, nodeActors)
    } finally {
      stopExecution
    }
  }

  def stopExecution {
    if (!system.isTerminated) {
      log.info("shutting down actor system")
      system.shutdown
    }
  }

}
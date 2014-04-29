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

import com.signalcollect.configuration.ActorSystemRegistry
import com.signalcollect.configuration.AkkaConfig
import com.signalcollect.util.LogHelper
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.event.Logging
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.async.Async.{ async, await }
import akka.actor.ActorRef
import com.signalcollect.util.ConfigProvider
import com.signalcollect.deployment.YarnDeployableAlgorithm
import scala.collection.JavaConversions._

class NewLeaderImpl(akkaPort: Int,
  kryoRegistrations: List[String],
  numberOfNodes: Int,

  kryoInit: String = "com.signalcollect.configuration.KryoInit") extends NewLeader with LogHelper {
  val system = ActorSystemRegistry.retrieve("SignalCollect").getOrElse(startActorSystem)
  val leaderactor = system.actorOf(Props[LeaderActor], "leaderactor")
  var executionStarted = false
  var executionFinished = false
  def start {
    async {
      waitForAllNodes
      executionStarted = true
      startExecution
      executionFinished = true
    }
  }

  def startExecution {
    val algorithm = ConfigProvider.config.getString("deployment.algorithm.class")
    val parameters = ConfigProvider.config.getConfig("deployment.algorithm.parameters").entrySet.map {
      entry => (entry.getKey, entry.getValue.unwrapped.toString)
    }.toMap
    try {
      val nodeActors = getNodeActors.toArray
      val algorithmObject = Class.forName(algorithm).newInstance.asInstanceOf[YarnDeployableAlgorithm]
      algorithmObject.execute(parameters, nodeActors)
    } finally {
    }
  }

  def getActorRef(): ActorRef = {
    leaderactor
  }

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

  def waitForAllNodes {
    while (!allNodesRunning) {
      Thread.sleep(100)
    }
  }

  def allNodesRunning: Boolean = {
    val nodesRunning = NodeAddresses.getNumberOfNodes == numberOfNodes
    nodesRunning
  }

  def akkaConfig(akkaPort: Int, kryoRegistrations: List[String]) = AkkaConfig.get(
    akkaMessageCompression = true,
    serializeMessages = true,
    loggingLevel = Logging.WarningLevel, //Logging.DebugLevel,Logging.WarningLevel
    kryoRegistrations = kryoRegistrations,
    kryoInitializer = kryoInit,
    port = akkaPort)

  def getNodeActors: List[ActorRef] = {
    val nodeActors = NodeAddresses.getAll.map(nodeAddress => system.actorFor(nodeAddress))
    nodeActors
  }
}

class LeaderActor extends Actor {
  override def receive = {
    case address: String => NodeAddresses.add(address)
    case actor: ActorRef => println("received actorRef")
    case _ => println("received unexpected message")
  }
  def test {

  }
}


object NodeAddresses {
  private var addresses: List[String] = Nil
  def getAll: List[String] = {
    synchronized {
      addresses
    }
  }
  def add(address: String) {
    synchronized {
      addresses = address :: addresses
    }
  }
  def getNumberOfNodes: Int = {
    synchronized {
      addresses.size
    }
  }
  def clear = {
    synchronized {
      addresses = Nil
    }
  }
}



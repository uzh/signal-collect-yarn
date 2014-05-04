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
import com.signalcollect.nodeprovisioning.AkkaHelper

class NewLeaderImpl(akkaPort: Int,
  kryoRegistrations: List[String],
  numberOfNodes: Int,

  kryoInit: String = "com.signalcollect.configuration.KryoInit") extends NewLeader with LogHelper {
  val system = ActorSystemRegistry.retrieve("SignalCollect").getOrElse(startActorSystem)
  val leaderactor = system.actorOf(Props[LeaderActor], "leaderactor")
  private var executionStarted = false
  private var executionFinished = false
  
  def isExecutionStarted = executionStarted
  def isExecutionFinished = executionFinished
  
  def start {
    async {
      waitForAllNodes
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
    executionStarted = true
  }

  def allNodesRunning: Boolean = {
    ActorAddresses.getNumberOfNodes == numberOfNodes
    
  }

  def shutdownAllNodes {
    getShutdownActors.foreach(_ ! "shutdown")
  }

  def akkaConfig(akkaPort: Int, kryoRegistrations: List[String]) = AkkaConfig.get(
    akkaMessageCompression = true,
    serializeMessages = true,
    loggingLevel = Logging.WarningLevel, //Logging.DebugLevel,Logging.WarningLevel
    kryoRegistrations = kryoRegistrations,
    kryoInitializer = kryoInit,
    port = akkaPort)

  def getNodeActors: List[ActorRef] = {
    val nodeActors = ActorAddresses.getNodeActorAddresses.map(nodeAddress => system.actorFor(nodeAddress))
    nodeActors
  }
  
  def getShutdownActors: List[ActorRef] = {
    val shutdownActors = ActorAddresses.getShutdownAddresses.map(address => system.actorFor(address))
    shutdownActors
  }
  
  
}

class LeaderActor extends Actor {
  override def receive = {
    case address: String => filterAddress(address)
    case _ => println("received unexpected message")
  }

  def filterAddress(address: String) {
    address match {
      case nodeactor if nodeactor.contains("NodeActor") => ActorAddresses.addNodeActorAddress(address)
      case shutdown if shutdown.contains("shutdown") => ActorAddresses.addShutdownAddress(address)
      case _ =>
    }
  }
}

object ActorAddresses {
  private var nodeActorAddresses: List[String] = Nil
  private var shutdownAddresses: List[String] = Nil
  def getNodeActorAddresses: List[String] = {
    synchronized {
      nodeActorAddresses
    }
  }
  def getShutdownAddresses: List[String] = {
    synchronized {
      shutdownAddresses
    }
  }
  def addNodeActorAddress(address: String) {
    synchronized {
      (
        nodeActorAddresses = address :: nodeActorAddresses)
    }
  }
  def addShutdownAddress(address: String) {
    synchronized {
      (
        shutdownAddresses = address :: shutdownAddresses)
    }
  }
  def getNumberOfNodes: Int = {
    synchronized {
      nodeActorAddresses.size
    }
  }
  def clear = {
    synchronized {
      nodeActorAddresses = Nil
      shutdownAddresses = Nil
    }
  }
}



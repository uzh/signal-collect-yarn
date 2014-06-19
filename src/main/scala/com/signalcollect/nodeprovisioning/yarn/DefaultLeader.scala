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
import com.signalcollect.deployment.DeployableAlgorithm
import scala.collection.JavaConversions._
import com.signalcollect.nodeprovisioning.AkkaHelper
import com.typesafe.config.Config
import com.signalcollect.deployment.DeploymentConfiguration

class DefaultLeader(
  akkaConfig: Config = AkkaConfigCreator.getConfig(2552),
  deploymentConfig: DeploymentConfiguration) extends Leader with LogHelper {
  val system = ActorSystemRegistry.retrieve("SignalCollect").getOrElse(startActorSystem)
  val leaderactor = system.actorOf(Props[LeaderActor], "leaderactor")
  val leaderAddress = AkkaHelper.getRemoteAddress(leaderactor, system)
  println(s"leaderAddress is $leaderAddress")
  private var executionStarted = false
  private var executionFinished = false

  def isExecutionStarted = executionStarted
  def isExecutionFinished = executionFinished

  def start {
    async {
      waitForAllNodes
      startExecution
      executionFinished = true
      shutdown
    }
  }

  def startExecution {
    val algorithm = deploymentConfig.algorithm
    val parameters = deploymentConfig.algorithmParameters
    val nodeActors = getNodeActors.toArray
    val algorithmObject = Class.forName(algorithm).newInstance.asInstanceOf[DeployableAlgorithm]
    println(s"start algorithm: $algorithm")
    algorithmObject.execute(parameters, Some(nodeActors), Some(system))
  }

  def shutdown {
    println("leader is shuttingdown")
    try {
      println("tell all ContainerNodes to shutdown")
      val shutdownActor = getShutdownActors.foreach(_ ! "shutdown")
    } finally {
      if (!system.isTerminated) {
        system.shutdown
        system.awaitTermination
        ActorSystemRegistry.remove(system)
      }
    }
  }

  def getActorRef(): ActorRef = {
    leaderactor
  }

  def startActorSystem: ActorSystem = {
    try {
      println("start actorsystem")
      val system = ActorSystem("SignalCollect", akkaConfig)
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
    ActorAddresses.getNumberOfNodes == deploymentConfig.numberOfNodes

  }

  def shutdownAllNodes {
    getShutdownActors.foreach(_ ! "shutdown")
  }

  def getNodeActors: List[ActorRef] = {
    println("get Node actors called")
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
    println(s"received $address")
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



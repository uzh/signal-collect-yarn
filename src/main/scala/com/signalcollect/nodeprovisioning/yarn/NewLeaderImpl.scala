package com.signalcollect.nodeprovisioning.yarn

import com.signalcollect.configuration.ActorSystemRegistry
import com.signalcollect.configuration.AkkaConfig
import com.signalcollect.util.LogHelper

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.event.Logging

import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.async.Async.{ async, await }

class NewLeaderImpl(akkaPort: Int, kryoRegistrations: List[String], numberOfNodes: Int, kryoInit: String = "com.signalcollect.configuration.KryoInit") extends NewLeader with LogHelper {
  val system = ActorSystemRegistry.retrieve("SignalCollect").getOrElse(startActorSystem)
  var executionStarted = false
  def start {
    async {
      waitForAllNodes
      executionStarted = true
      
    }
  }

  def getActorRef(): ActorRef = {
    system.actorOf(Props[LeaderActor], "leaderactor")
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
    case whatever => println("received unexpected message")
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



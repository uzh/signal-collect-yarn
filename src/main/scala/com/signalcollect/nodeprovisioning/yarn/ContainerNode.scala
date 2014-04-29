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
import com.signalcollect.nodeprovisioning.AkkaHelper
import akka.actor.ActorRef
import akka.actor.Actor
import akka.actor.Props
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.async.Async.{ async, await }
import com.signalcollect.nodeprovisioning.DefaultNodeActor
import com.signalcollect.nodeprovisioning.NodeActorCreator

class ContainerNode(id: Int,
  numberOfNodes: Int,
  leaderIp: String,
  basePort: Int = 2552,
  kryoRegistrations: List[String] = Nil,
  kryoInit: String = "com.signalcollect.configuration.KryoInit") {

  val akkaPort = basePort + id + 1
  val leaderAddress = s"akka://SignalCollect@$leaderIp:$basePort/user/leaderactor"
  val system = ActorSystemRegistry.retrieve("SignalCollect").getOrElse(startActorSystem)
  val shutdownActor = system.actorOf(Props[ShutdownActor], s"shutdownactor$id")
  val nodeControllerCreator = NodeActorCreator(id, numberOfNodes, None)
  val nodeActor = system.actorOf(Props[DefaultNodeActor].withCreator(
    nodeControllerCreator.create), name = "DefaultNodeActor" + id.toString)

  private var terminated = false

  def getShutdownActor(): ActorRef = {
    shutdownActor
  }

  def getNodeActor(): ActorRef = {
    nodeActor
  }

  def getLeaderActor(): ActorRef = {
    system.actorFor(leaderAddress)
  }
  
  def isTerminated: Boolean = terminated

  def start {
    async {
      while (!ShutdownHelper.shuttingdown) {
        Thread.sleep(100)
      }
      terminated = true
    }
  }

  def register {
    getLeaderActor ! AkkaHelper.getRemoteAddress(nodeActor, system)
  }

  def startActorSystem: ActorSystem = {
    val system = ActorSystem("SignalCollect", akkaConfig(akkaPort, kryoRegistrations))
    ActorSystemRegistry.register(system)
    system

  }

  def akkaConfig(akkaPort: Int, kryoRegistrations: List[String]) = AkkaConfig.get(
    akkaMessageCompression = true,
    serializeMessages = true,
    loggingLevel = Logging.WarningLevel, //Logging.DebugLevel,Logging.WarningLevel
    kryoRegistrations = kryoRegistrations,
    kryoInitializer = kryoInit,
    port = akkaPort)
}

class ShutdownActor extends Actor {
  override def receive = {
    case "shutdown" => ShutdownHelper.shutdown
    case whatever => println("received unexpected message")
  }
}

object ShutdownHelper {
  private var shutdownNow = false

  def shutdown = {
    synchronized {
      shutdownNow = true
    }
  }

  def shuttingdown: Boolean = {
    shutdownNow
  }

  def reset {
    synchronized {
      shutdownNow = false
    }
  }
}
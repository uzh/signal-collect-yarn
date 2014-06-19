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
import com.typesafe.config.Config
import com.signalcollect.util.ConfigProvider

trait ContainerNode {
  def start
  def shutdown
  def waitForTermination
  def isSuccessful: Boolean
}

class DefaultContainerNode(id: Int,
  numberOfNodes: Int,
  leaderIp: String,
  basePort: Int,
  akkaConfig: Config) extends ContainerNode {

  val leaderAddress = s"akka.tcp://SignalCollect@$leaderIp:$basePort/user/leaderactor"
  println("get actorsystem")
  val system = ActorSystemRegistry.retrieve("SignalCollect").getOrElse(startActorSystem)
  println("start shutdownactor")
  val shutdownActor = system.actorOf(Props[ShutdownActor], s"shutdownactor$id")
  println(s"start nodeActor with id $id on $numberOfNodes nodes")
  val nodeActor = system.actorOf(Props(classOf[DefaultNodeActor], id.toString, id, numberOfNodes, None), name = id.toString + "DefaultNodeActor")

  private var terminated = false
  
  private var successful = false

  def getShutdownActor(): ActorRef = {
    shutdownActor
  }

  def getNodeActor(): ActorRef = {
    nodeActor
  }

  def getLeaderActor(): ActorRef = {
    println(s"leaderAddress is $leaderAddress")
    system.actorFor(leaderAddress)
  }

  def isTerminated: Boolean = terminated
  
  def isSuccessful: Boolean = successful

  def start {
    async {
      try {
        println("register container")
        register
        println("wait for termination")
        waitForTermination
        successful = true
      } catch {
        case e: Throwable => {
          println("catched Exception")
          throw e}
      } finally {
        terminated = true
        shutdown
      }
    }
  }

  def register {
    getLeaderActor ! AkkaHelper.getRemoteAddress(nodeActor, system)
    getLeaderActor ! AkkaHelper.getRemoteAddress(shutdownActor, system)
  }

  def waitForTermination {
    val begin = System.currentTimeMillis()
    while (!ShutdownHelper.shuttingdown && timeoutNotReached(begin)) {
      Thread.sleep(100)
    }
    terminated = true
  }
  
  def timeoutNotReached(begin: Long): Boolean = {
    val timeout = ConfigProvider.config.getInt("deployment.timeout")
    (System.currentTimeMillis() - begin) / 1000 < timeout
  }

  def shutdown {
    if (!system.isTerminated) {
      system.shutdown
      system.awaitTermination
      ActorSystemRegistry.remove(system)
    }
  }

  def startActorSystem: ActorSystem = {
    try {
    println("start Actorsystem")
    val system = ActorSystem("SignalCollect", akkaConfig)
    println("register actorsystem")
    ActorSystemRegistry.register(system)
    println("registered actorsystem")
    } catch {
      case e:Throwable => {
        println("failed to start Actorsystem")
        throw e
      }
    }
    ActorSystemRegistry.retrieve("SignalCollect").get
  }

}

class ShutdownActor extends Actor {
  override def receive = {
    case "shutdown" => {
      println("shutdown received")
      ShutdownHelper.shutdown
    }
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
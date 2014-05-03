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

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.mutable.SpecificationWithJUnit
import com.signalcollect.configuration.ActorSystemRegistry
import akka.actor.ActorRef
import org.specs2.mutable.After
import akka.actor.ActorSystem
import org.specs2.specification.AfterExample
import org.specs2.specification.Scope
import java.net.InetAddress
import akka.actor.Props
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.async.Async.{ async, await }
import com.signalcollect.nodeprovisioning.AkkaHelper

@RunWith(classOf[JUnitRunner])
class NewLeaderSpec extends SpecificationWithJUnit {
  "Leader" should {
    sequential //this is preventing the tests from being executed parallel

    "be started in" in new StopActorSystemAfter {
      val akkaPort = 2552
      val leader: NewLeader = new NewLeaderImpl(akkaPort, Nil, 1)
      ActorSystemRegistry.retrieve("SignalCollect").isDefined === true
    }

    "create LeaderActor" in new LeaderScope {
      leaderActor.path.toString.contains("leaderactor")
    }

    "detect if all nodes are ready " in new LeaderScope {

      ActorAddresses.clear
      async { //is needed because it is blocking
        leader.waitForAllNodes
      }
      leader.isExecutionStarted === false
      leader.allNodesRunning === false
      val address = s"akka://SignalCollect@$ip:2553/user/DefaultNodeActor$id"
      leaderActor ! address
      Thread.sleep(1000)
      leader.allNodesRunning === true
      leader.isExecutionStarted === true

      leader.getNodeActors must not(throwAn[Exception])
      val nodeActors = leader.getNodeActors
      nodeActors must not be empty
      nodeActors.head.path.toString === s"akka://SignalCollect@$ip:2553/user/DefaultNodeActor$id"
    }

    "filter address on DefaultNodeActor" in new LeaderScope {
      ActorAddresses.clear
      async { //is needed because wait is blocking
        leader.waitForAllNodes
      }
      val invalidAddress = "akka://SignalCollect@invalid"
      leaderActor ! invalidAddress
      ActorAddresses.getNodeActorAddresses.isEmpty === true
    }

    "save shutdown address" in new LeaderScope {
      ActorAddresses.clear
      async { //is needed because wait is blocking
        leader.waitForAllNodes
      }
      val shutdownAddress = s"akka://SignalCollect@$ip:2553/user/shutdownactor$id"
      leaderActor ! shutdownAddress
      ActorAddresses.getShutdownAddresses.isEmpty === false
    }

    "clear ActorAddresses" in new LeaderScope {
      ActorAddresses.clear
      ActorAddresses.getNodeActorAddresses.isEmpty === true
      ActorAddresses.getShutdownAddresses.isEmpty === true
    }
  }

  //integration of leader and container
  "Leader and ContainerNode" should {

    sequential //this is preventing the tests from being executed parallel
    "start execution when all registered" in new LeaderContainerScope {
      container.register
      Thread.sleep(1000)
      leader.isExecutionStarted === true
      var cnt = 0
      while (!leader.isExecutionFinished && cnt < 1000) {
        Thread.sleep(100)
        cnt += 1
      }
      leader.isExecutionFinished === true
    }

    "get shutdownActors" in new LeaderContainerScope {
      container.register
      Thread.sleep(1000)
      leader.getShutdownActors.size === 1
      leader.getShutdownActors.head.path.toString.contains("shutdown") === true
    }

    "shutdown after execution" in new LeaderContainerScope {
      container.register
      Thread.sleep(1000)
      leader.isExecutionStarted === true
      var cnt = 0
      while (!leader.isExecutionFinished && cnt < 1000) {
        Thread.sleep(100)
        cnt += 1
      }
      val shutdownActor = leader.getShutdownActors.head
      println(shutdownActor.isTerminated)
      shutdownActor ! "shutdown"
      Thread.sleep(1000)
      ShutdownHelper.shuttingdown === true
    }
  }

}

trait StopActorSystemAfter extends After {
  override def after = {
    ActorSystemRegistry.retrieve("SignalCollect") match {
      case Some(system) => clearSystem(system)
      case None =>
    }
  }

  def clearSystem(system: ActorSystem) {
	system.shutdown
	system.awaitTermination
    ActorSystemRegistry.remove(system)
    
  }
}

trait LeaderScope extends StopActorSystemAfter {
  val akkaPort = 2552
  val ip = InetAddress.getLocalHost.getHostAddress
  val id = 0
  val leader = new NewLeaderImpl(akkaPort, Nil, 1)
  val leaderActor: ActorRef = leader.getActorRef()

  abstract override def after {
    super.after
    ActorAddresses.clear
  }
}
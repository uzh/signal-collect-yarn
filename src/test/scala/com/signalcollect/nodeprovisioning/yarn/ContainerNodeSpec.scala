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
import java.net.InetAddress
import org.specs2.specification.Scope
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.async.Async.{ async, await }

@RunWith(classOf[JUnitRunner])
class ContainerNodeSpec extends SpecificationWithJUnit {
  "ContainerNode creation" should {
    sequential
    "be created" in new ContainerScope {
      container must not be None
    }
    "container node should start actor system" in new ContainerScope {
      container must not be None
      ActorSystemRegistry.retrieve("SignalCollect").isDefined === true

    }

    "create shutdown actor" in new ContainerScope {
      val actor = container.getShutdownActor
      actor must not be None
    }

    "receive shutdown message" in new ContainerScope {
      container must not be None
      val actor = container.getShutdownActor
      actor ! "shutdown"
      ShutdownHelper.shuttingdown === true
    }

    "wait for shutdown message" in new ContainerScope {
      ShutdownHelper.reset
      ShutdownHelper.shuttingdown === false
      async{
    	  container.waitForTermination
      }
      container.isTerminated === false
      container.getShutdownActor ! "shutdown"
      Thread.sleep(1000)
      container.isTerminated === true
      container.shutdown
      ActorSystemRegistry.retrieve("SignalCollect").isDefined === false
    }

    "get NodeActor" in new ContainerScope {
      container.getNodeActor must not be None
    }

    "get LeaderActor" in new LeaderContainerScope {
      val leaderActor = container.getLeaderActor
      leaderActor.path.toString === "akka://SignalCollect/user/leaderactor"
    }
    
    "register with leader" in new LeaderContainerScope{
      container.register
      ActorAddresses.getNodeActorAddresses.exists(_ .contains("DefaultNodeActor")) === true
      ActorAddresses.getShutdownAddresses.exists(_ .contains("shutdown")) === true
    }
  }

}

trait ContainerScope extends StopActorSystemAfter {
  val leaderIp = InetAddress.getLocalHost().getHostAddress()
  val container = new DefaultContainerNode(0, 1, leaderIp)

}
trait LeaderContainerScope extends StopActorSystemAfter {
  ActorAddresses.clear
  ShutdownHelper.reset
  val leaderIp = InetAddress.getLocalHost().getHostAddress()
  val leader = new DefaultLeader(2552, Nil, 1)
  leader.start
  val container = new DefaultContainerNode(0, 1, leaderIp)
  container.start
  
  abstract override def  after {
    super.after
    ActorAddresses.clear
    ShutdownHelper.reset
  }

}
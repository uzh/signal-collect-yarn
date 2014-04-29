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

@RunWith(classOf[JUnitRunner])
class NewLeaderSpec extends SpecificationWithJUnit {
  "Leader" should {
	  sequential

    "be started in" in new StopActorSystemAfter {
	    println("test1")
      val akkaPort = 2552
  
      val leader: NewLeader = new NewLeaderImpl(akkaPort, Nil, 1)
      ActorSystemRegistry.retrieve("SignalCollect").isDefined === true
    }

    "create LeaderActor" in new StopActorSystemAfter {
      println("test2")
      val akkaPort = 2552
      val ip = "0.0.0.0"
      val id = 0
      val leader2 = new NewLeaderImpl(akkaPort, Nil, 1)
      val leaderActor: ActorRef = leader2.getActorRef()
      leaderActor must not be None
    }

    "detect if all nodes are ready " in new StopActorSystemAfter {
      println("test3")
      val akkaPort = 2552
      val ip = "0.0.0.0"
      val id = 0
      val leader2 = new NewLeaderImpl(akkaPort, Nil, 1)
      val leaderActor: ActorRef = leader2.getActorRef()
      NodeAddresses.clear
      leader2.start
      leader2.executionStarted === false
      leader2.allNodesRunning === false
      val address = s"akka://SignalCollect@$ip:2553/user/DefaultNodeActor$id"
      leaderActor ! address
      Thread.sleep(1000)
      leader2.allNodesRunning === true
      leader2.executionStarted === true

      leader2.getNodeActors must not(throwAn[Exception])
      val nodeActors = leader2.getNodeActors
      nodeActors must not be empty
      nodeActors.head.path.toString === s"akka://SignalCollect@$ip:2553/user/DefaultNodeActor$id"

    }

  }

}

trait StopActorSystemAfter extends Scope {
  def after = {
    ActorSystemRegistry.retrieve("SignalCollect") match {
      case Some(system) => clearSystem(system)
      case None => 
    }

  }

  def clearSystem(system: ActorSystem) {
    ActorSystemRegistry.remove(system)
    system.shutdown
  }
}
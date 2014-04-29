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

@RunWith(classOf[JUnitRunner])
class NewLeaderSpec extends SpecificationWithJUnit {
  "Leader" should {
    sequential

    "be started in" in new StopActorSystemAfter {
      val akkaPort = 2552
      val leader: NewLeader = new NewLeaderImpl(akkaPort, Nil, 1)
      ActorSystemRegistry.retrieve("SignalCollect").isDefined === true
    }

    "create LeaderActor" in new LeaderScope {
      leaderActor.path.toString.contains("leaderactor")
    }

    "detect if all nodes are ready " in new LeaderScope {

      NodeAddresses.clear
      leader.start
      leader.executionStarted === false
      leader.allNodesRunning === false
      val address = s"akka://SignalCollect@$ip:2553/user/DefaultNodeActor$id"
      leaderActor ! address
      Thread.sleep(1000)
      leader.allNodesRunning === true
      leader.executionStarted === true

      leader.getNodeActors must not(throwAn[Exception])
      val nodeActors = leader.getNodeActors
      nodeActors must not be empty
      nodeActors.head.path.toString === s"akka://SignalCollect@$ip:2553/user/DefaultNodeActor$id"
    }
    
    "receive ContainerHost" in new LeaderScope {
      leaderActor ! ActorSystemRegistry.retrieve("SignalCollect").get.actorOf(Props[ShutdownActor], s"shutdownactor$id")
    }
    
    
    "start execution when all registered" in new LeaderContainerScope {
      container.register
      Thread.sleep(1000)
      leader.executionStarted === true
      var cnt = 0
      while (!leader.executionFinished && cnt < 100) {
        Thread.sleep(100)
        cnt += 1
      }
      leader.executionFinished === true
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
    ActorSystemRegistry.remove(system)
    system.shutdown
  }
}

trait LeaderScope extends StopActorSystemAfter {
  val akkaPort = 2552
  val ip = InetAddress.getLocalHost().getHostAddress()
  val id = 0
  val leader = new NewLeaderImpl(akkaPort, Nil, 1)
  val leaderActor: ActorRef = leader.getActorRef()
  
  abstract override def after {
    super.after
    NodeAddresses.clear
  }
}
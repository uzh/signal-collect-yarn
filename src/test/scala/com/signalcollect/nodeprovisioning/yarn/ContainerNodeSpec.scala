package com.signalcollect.nodeprovisioning.yarn

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.mutable.SpecificationWithJUnit
import com.signalcollect.configuration.ActorSystemRegistry
import java.net.InetAddress
import org.specs2.specification.Scope

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
      ShutdownHelper.isShutdownNow === true
    }

    "wait for shutdown message" in new ContainerScope {
      ShutdownHelper.reset
      ShutdownHelper.isShutdownNow === false
      container.start
      container.terminated === false
      container.getShutdownActor ! "shutdown"
      Thread.sleep(1000)
      container.terminated === true
    }

    "get NodeActor" in new ContainerScope {
      container.getNodeActor must not be None
    }

    "get LeaderActor" in new LeaderContainerScope {
      val leaderActor = container.getLeaderActor
      leaderActor must not be None
    }
  }

}

trait ContainerScope extends StopActorSystemAfter {
  val leaderIp = InetAddress.getLocalHost().getHostAddress()
  val container = new ContainerNode(0, 1, leaderIp)

}
trait LeaderContainerScope extends StopActorSystemAfter {
  val leaderIp = InetAddress.getLocalHost().getHostAddress()
  val leader = new NewLeaderImpl(2552, Nil, 1)
  leader.start
  val container = new ContainerNode(0, 1, leaderIp)

}
package com.signalcollect.nodeprovisioning.yarn

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.mutable.SpecificationWithJUnit
import com.signalcollect.configuration.ActorSystemRegistry
import akka.actor.ActorRef

@RunWith(classOf[JUnitRunner])
class NewLeaderSpec extends SpecificationWithJUnit {
	
  "Leader" should {
    val akkaPort = 2552
    val leader: NewLeader = new NewLeaderImpl(akkaPort, Nil)
    
    "be started in" in {
      leader.start must not(throwAn[Exception])
      ActorSystemRegistry.retrieve("SignalCollect").isDefined === true
    }
    
    val leader2 = new NewLeaderImpl(akkaPort, Nil)
    val leaderActor: ActorRef = leader2.getActorRef()
    "create LeaderActor" in {
      leaderActor !== null
    }
    
    "leaderActor should save adresses" in {
      val address = "akka://SignalCollect@127.0.0.1:2553/user/DefaultNodeActor0"
      leaderActor ! address
      NodeAddresses.getAll.exists(address.equals(_)) === true
    }
  } 
}
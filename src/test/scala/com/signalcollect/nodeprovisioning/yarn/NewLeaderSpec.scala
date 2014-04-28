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
    val leader: NewLeader = new NewLeaderImpl(akkaPort, Nil, 1)
    
    "be started in" in {
      ActorSystemRegistry.retrieve("SignalCollect").isDefined === true
    }
    
    val leader2 = new NewLeaderImpl(akkaPort, Nil,1)
    val leaderActor: ActorRef = leader2.getActorRef()
    "create LeaderActor" in {
      leaderActor !== null
    }
    
    "detect if all nodes are ready " in {
      NodeAddresses.clear
      leader2.start
      leader2.executionStarted === false
      leader2.allNodesRunning === false
      val address = "akka://SignalCollect@127.0.0.1:2553/user/DefaultNodeActor0"
      leaderActor ! address
      Thread.sleep(1000)
      leader2.allNodesRunning === true
      leader2.executionStarted === true
      
    }
    
    "get NodeActors" in {
      leader2.getNodeActors 
    }
    
   
  } 
}
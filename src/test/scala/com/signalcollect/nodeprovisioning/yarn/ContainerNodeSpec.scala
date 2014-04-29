package com.signalcollect.nodeprovisioning.yarn

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.mutable.SpecificationWithJUnit
import com.signalcollect.configuration.ActorSystemRegistry

@RunWith(classOf[JUnitRunner])
class ContainerNodeSpec extends SpecificationWithJUnit {
 "ContainerNode creation" should {
   "be created" in  {
    val container = new ContainerNode(0)
    container must not be None
   }
   
   "container node should start actor system" in  {
     val id = 0
     val baseport = 2552
     val container = new ContainerNode(id = id, baseport = baseport)
     container must not be None
     ActorSystemRegistry.retrieve("SignalCollect").isDefined === true
    
   }
   
   "create shutdown actor" in {
     val container = new ContainerNode(0)
     val actor = container.getShutdownActor
     actor must not be None	
   }
   
   "receive shutdown message" in  {
     val container = new ContainerNode(0)
     container must not be None
     val actor = container.getShutdownActor 
     actor ! "shutdown"
     ShutdownHelper.isShutdownNow === true
   }
   
   "wait for shutdown message" in {
      ShutdownHelper.reset
      ShutdownHelper.isShutdownNow === false
      val container = new ContainerNode(0)
      container.start
      container.terminated === false
      container.getShutdownActor ! "shutdown"
      Thread.sleep(1000)
      container.terminated === true
   }
 }

}
package com.signalcollect.nodeprovisioning.yarn

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.mutable.SpecificationWithJUnit
import com.signalcollect.configuration.ActorSystemRegistry

@RunWith(classOf[JUnitRunner])
class ContainerNodeSpec extends SpecificationWithJUnit with StopActorSystemAfter {
 "ContainerNode creation" should {
//   "be created" in  {
//    val container = new ContainerNode(0)
//    container must not be None
//   }
   
   "container node should start actor system" in  {
     val id = 0
     val baseport = 2552
     val container = new ContainerNode(id = id, baseport = baseport)
     container must not be None
     ActorSystemRegistry.retrieve("SignalCollect").isDefined === true
    
   }
   
   "receive shutdown message" in  {
     val container = new ContainerNode(0)
     container must not be None
   }
   
 }
 

}
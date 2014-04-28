package com.signalcollect.nodeprovisioning.yarn

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.mutable.SpecificationWithJUnit

@RunWith(classOf[JUnitRunner])
class ContainerNodeSpec extends SpecificationWithJUnit {
 "ContainerNode" should {
   "be created" in {
    val container = new ContainerNode()
    container must not be None
   }
 }
}
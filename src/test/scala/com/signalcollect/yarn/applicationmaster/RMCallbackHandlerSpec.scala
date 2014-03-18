package com.signalcollect.yarn.applicationmaster

import org.apache.hadoop.yarn.api.records._
import org.mockito.Matchers
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.mock._
import org.specs2.mutable.SpecificationWithJUnit

@RunWith(classOf[JUnitRunner])
class RMCallbackHandlerSpec extends SpecificationWithJUnit with Mockito {
//  "RMCallbackHandler" should {
//    
//    val callback = new RMCallbackHandler()
//    
//    "be created" in {
//      callback !== null
//    }
//    
//    "onContainersAllocated start container" in {
//      val containers = mock[java.util.List[Container]]
//      val container = mock[Container]
//      val id = mock[ContainerId]
//      containers.size() returns 1
//      containers.get(0) returns container
//      container.getId() returns id
//      id.toString() returns "containerId1"
//      callback.onContainersAllocated(containers) must not(throwAn[Exception])
//    }
//    
//  }
}
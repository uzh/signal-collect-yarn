package com.signalcollect.nodeprovisioning.yarn

import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import org.specs2.mutable.SpecificationWithJUnit

@RunWith(classOf[JUnitRunner])
class ApplicationMasterSpec extends SpecificationWithJUnit {
  "ApplicationMaster" should {
    "be created" in {
      ApplicationMaster.run() must not(throwAn[Exception])
    }
  }
}
package com.signalcollect.nodeprovisioning.yarn

import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import org.specs2.mutable.SpecificationWithJUnit
import org.apache.hadoop.conf.Configuration

@RunWith(classOf[JUnitRunner])
class ApplicationMasterSpec extends SpecificationWithJUnit {
  args(skipAll=true)
  "ApplicationMaster" should {
    "be created" in {
      val cluster = MiniCluster.cluster
      val config = new Configuration(cluster.getConfig())
      val applicationMaster = new ApplicationMaster(config)
      applicationMaster.run() must not(throwAn[Exception])
    }
  }
}
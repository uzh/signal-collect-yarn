package com.signalcollect.nodeprovisioning.yarn

import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import org.specs2.mutable.SpecificationWithJUnit

@RunWith(classOf[JUnitRunner])
class YarnApplicationFactorySpec extends SpecificationWithJUnit {
  "YarnApplicationFactory" should {
    val factory = new DefaultYarnClientFactory()
    val config = ConfigFactory.load("test-deployment")
    val yarnClient = factory.getYarnClient(config)
    
    "create a new Application" in {
      val application = YarnApplicationFactory.getApplication(config, yarnClient)
      application !== null
      application.getNewApplicationResponse().getApplicationId() !== null
    }

  }
}
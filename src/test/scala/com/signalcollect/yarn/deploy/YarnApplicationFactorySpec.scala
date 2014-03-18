package com.signalcollect.yarn.deploy

import com.signalcollect.yarn.deployment.YarnApplicationFactory;
import com.signalcollect.yarn.deployment.YarnClientFactory;
import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config

import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import org.specs2.mutable.SpecificationWithJUnit

@RunWith(classOf[JUnitRunner])
class YarnApplicationFactorySpec extends SpecificationWithJUnit {
  "YarnApplicationFactory" should {
    val typesafeConfig = ConfigFactory.load("test-deployment")
    val yarnClient = YarnClientFactory.yarnClient
    
    "create a new Application" in {
      val application = YarnApplicationFactory.getApplication(typesafeConfig, yarnClient)
      application !== null
      application.getNewApplicationResponse().getApplicationId() !== null
    }

  }
}
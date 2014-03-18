package com.signalcollect.yarn.deploy

import com.signalcollect.yarn.deployment.YarnApplicationCreator
import com.signalcollect.yarn.deployment.YarnClientCreator
import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config

import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import org.specs2.mutable.SpecificationWithJUnit

@RunWith(classOf[JUnitRunner])
class YarnApplicationCreatorSpec extends SpecificationWithJUnit {
  "YarnApplicationFactory" should {
    val typesafeConfig = ConfigFactory.load("test-deployment")
    val yarnClient = YarnClientCreator.yarnClient
    
    "create a new Application" in {
      val application = YarnApplicationCreator.getApplication(typesafeConfig, yarnClient)
      application !== null
      application.getNewApplicationResponse().getApplicationId() !== null
    }

  }
}
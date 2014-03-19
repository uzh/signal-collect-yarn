package com.signalcollect.yarn.deployment

import com.signalcollect.yarn.deployment.YarnClientCreator
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import org.specs2.mutable.SpecificationWithJUnit

@RunWith(classOf[JUnitRunner])
class YarnClientCreatorSpec extends SpecificationWithJUnit {
  "YarnClientFactory" should {

    val typesafeConfig = ConfigFactory.load("test-deployment")
    val badConfig = ConfigFactory.load("bad-test-deployment")
    
    "a valid config should contain RM Host and Address" in {
      val yarnConfig = YarnClientCreator.yarnClient.getConfig()
      val yarnHost = "yarn.resourcemanager.host"
      val yarnAddress = "yarn.resourcemanager.address"
      val deploymentHost = typesafeConfig.getString("deployment." + yarnHost)
      val deploymentAddress = typesafeConfig.getString("deployment." + yarnAddress)
      yarnConfig.get(yarnHost) must be(deploymentHost)
      yarnConfig.get(yarnAddress) must be(deploymentAddress)
    }
  }
}
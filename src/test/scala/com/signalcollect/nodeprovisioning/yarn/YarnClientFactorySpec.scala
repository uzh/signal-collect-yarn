package com.signalcollect.nodeprovisioning.yarn

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import org.specs2.mutable.SpecificationWithJUnit

@RunWith(classOf[JUnitRunner])
class YarnClientFactorySpec extends SpecificationWithJUnit {
  "YarnClientFactory" should {

    val typesafeConfig = ConfigFactory.load("test-deployment")
    val badConfig = ConfigFactory.load("bad-test-deployment")
    val factory: YarnClientFactory = new DefaultYarnClientFactory()

    "throw an IllegalArgumentEx if it is constructed with Null" in {
      factory.getYarnClient(null) must throwA[IllegalArgumentException]
    }

    "be instantiated with a Typesaf Config" in {
      factory.getYarnClient(typesafeConfig) must not(throwAn[Exception])
    }

    "typesafeConfig should be validated" in {
      factory.getYarnClient(badConfig) must throwA[IllegalArgumentException]
    }

    "a valid config should contain RM Host and Address" in {
      val yarnConfig = factory.getYarnClient(typesafeConfig).getConfig()
      val yarnHost = "yarn.resourcemanager.host"
      val yarnAddress = "yarn.resourcemanager.address"
      val deploymentHost = typesafeConfig.getString("deployment." + yarnHost)
      val deploymentAddress = typesafeConfig.getString("deployment." + yarnAddress)
      yarnConfig.get(yarnHost) must be(deploymentHost)
      yarnConfig.get(yarnAddress) must be(deploymentAddress)
    }
    "work with a MiniYarnClientFactory" in {
      val miniFactory: YarnClientFactory = new MiniYarnClientFactory()
      miniFactory !== null
    }
  }
}
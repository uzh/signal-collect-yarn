package com.signalcollect.nodeprovisioning.yarn

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import org.specs2.mutable.SpecificationWithJUnit

@RunWith(classOf[JUnitRunner])
class YarnSubmissionContextFactorySpec extends SpecificationWithJUnit {
  "YarnSubmissionContextFactory" should {
    val config = ConfigFactory.load("test-deployment")
    val yarnClientFactory = new MiniYarnClientFactory()
    lazy val yarnClient = yarnClientFactory.getYarnClient(config)
    lazy val application = YarnApplicationFactory.getApplication(config, yarnClient)
    "call Factory" in {
      val factory = new YarnSubmissionContextFactory(yarnClient, config, application)
      val context =factory.getSubmissionContext()
      context.getApplicationName() === config.getString("deployment.applicationName")
    }
  }
}
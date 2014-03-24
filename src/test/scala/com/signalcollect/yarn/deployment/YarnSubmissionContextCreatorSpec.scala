package com.signalcollect.yarn.deployment

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import org.specs2.mutable.SpecificationWithJUnit

@RunWith(classOf[JUnitRunner])
class YarnSubmissionContextFactorySpec extends SpecificationWithJUnit {
  "YarnSubmissionContextFactory" should {
    val config = ConfigFactory.load("test-deployment")
    lazy val yarnClient = YarnClientCreator.yarnClient
    lazy val application = YarnApplicationCreator.getApplication(config, yarnClient)
    "call Factory" in {
      val launchSettings = new LaunchSettings()
      val factory = new YarnSubmissionContextCreator(yarnClient, application, launchSettings)
      val context =factory.getSubmissionContext()
      context.getApplicationName() === config.getString("deployment.applicationName")
    }
  }
}
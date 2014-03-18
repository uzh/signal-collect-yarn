package com.signalcollect.yarn.deploy

import com.signalcollect.yarn.deployment.YarnApplicationCreator;
import com.signalcollect.yarn.deployment.YarnClientCreator;
import com.signalcollect.yarn.deployment.YarnSubmissionContextCreator;
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
      val factory = new YarnSubmissionContextCreator(yarnClient, config, application)
      val context =factory.getSubmissionContext()
      context.getApplicationName() === config.getString("deployment.applicationName")
    }
  }
}
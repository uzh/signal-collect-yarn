package com.signalcollect.yarn.applicationmaster

import com.signalcollect.util.LogHelper
import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.specs2.mutable.SpecificationWithJUnit
import com.signalcollect.yarn.deploy.YarnClientFactory
import com.signalcollect.yarn.deploy.YarnDeploymentClient
import scala.collection.JavaConversions._
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus

@RunWith(classOf[JUnitRunner])
class ApplicationMasterSpec extends SpecificationWithJUnit with LogHelper {
  args(skipAll = true)
  "ApplicationMaster" should {
    val typesafeConfig = ConfigFactory.load("test-deployment")
    val yarnClient = YarnClientFactory.yarnClient
    val client = new YarnDeploymentClient

    "run application successfull" in {
      client.submitApplication()
      var finished = false
      val apps = yarnClient.getApplications
      while (!finished) {
        if (apps.size() == 0) {
          Thread.sleep(10)
          log.debug("No Applications found")
        } else {
          log.debug(apps.size() + " application(s) found")
          val appReport = apps.get(apps.size -1)
          
          if (appReport.getYarnApplicationState() == YarnApplicationState.FINISHED) {
            finished = true
          }
        }
      }
      val appReport = apps.get(apps.size -1)
      println(appReport.getApplicationId())
      appReport.getFinalApplicationStatus() === FinalApplicationStatus.SUCCEEDED
    }
  }
}
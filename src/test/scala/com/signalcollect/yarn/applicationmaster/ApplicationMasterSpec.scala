package com.signalcollect.yarn.applicationmaster

import com.signalcollect.util.LogHelper
import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.specs2.mutable.SpecificationWithJUnit
import com.signalcollect.yarn.deployment.YarnClientCreator
import com.signalcollect.yarn.deployment.YarnDeploymentClient
import scala.collection.JavaConversions._
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus
import com.signalcollect.yarn.deployment.LaunchSettings
import com.signalcollect.yarn.deployment.MiniCluster
import java.nio.file.Path
import java.io.File
import com.signalcollect.yarn.deployment.TestLaunchSettings

@RunWith(classOf[JUnitRunner])
class ApplicationMasterSpec extends SpecificationWithJUnit{
  
  "ApplicationMaster" should {
//    val typesafeConfig = ConfigFactory.load("test-deployment")
//    val yarnClient = YarnClientCreator.yarnClient
//    val launchSettings = TestLaunchSettings.getSettingsForClass(ApplicationMaster.getClass())
//    val client = new YarnDeploymentClient(launchSettings)
//
//    "run application successfull" in {
//      val application = client.submitApplication()
//      var finished = false
//      while (!finished) {
//        Thread.sleep(1000)
//        val apps = yarnClient.getApplications.toList
//        if (apps.size() == 0) {
//          Thread.sleep(10)
//        } else {
//          if (apps.exists(_.getApplicationId().equals(application))) {
//            val appReport = apps.find(_.getApplicationId().equals(application)).get
//            val applicationState = appReport.getYarnApplicationState()
//            println("ApplicationState = " + applicationState)
//            if (applicationState == YarnApplicationState.FINISHED || applicationState == YarnApplicationState.FAILED) {
//              finished = true
//            }
//          }
//        }
//      }
//      val appReport = yarnClient.getApplications.toList.find(_.getApplicationId().equals(application)).get
//      appReport.getFinalApplicationStatus() === FinalApplicationStatus.SUCCEEDED
//    }
  }
}
package com.signalcollect.yarn.deployment

import com.signalcollect.yarn.applicationmaster.ApplicationMaster
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions._
import org.apache.hadoop.yarn.api.records.YarnApplicationState

object YarnDeployer extends App {
  val typesafeConfig = ConfigFactory.load("yarn-deployment")
  val yarnClient = YarnClientCreator.yarnClient
  val launchSettings = LaunchSettingsCreator.getSettingsForClass(ApplicationMaster.getClass())
  val client = new YarnDeploymentClient(launchSettings)
  val application = client.submitApplication()
  waitForTermination
  
  def waitForTermination {
    var finished = false
    while (!finished) {
      Thread.sleep(1000)
      val apps = yarnClient.getApplications.toList
      if (apps.size() == 0) {
        Thread.sleep(10)
      } else {
        if (apps.exists(_.getApplicationId().equals(application))) {
          val appReport = apps.find(_.getApplicationId().equals(application)).get
          val applicationState = appReport.getYarnApplicationState()
          println("ApplicationState = " + applicationState)
          if (applicationState == YarnApplicationState.FINISHED || applicationState == YarnApplicationState.FAILED) {
            finished = true
          }
        }
      }
    }
  }

}

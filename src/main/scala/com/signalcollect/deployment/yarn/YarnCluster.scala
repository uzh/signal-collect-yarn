package com.signalcollect.deployment.yarn

import com.signalcollect.yarn.applicationmaster.ApplicationMaster
import com.typesafe.config.ConfigFactory

import scala.collection.JavaConversions._

import org.apache.hadoop.yarn.api.records.YarnApplicationState

import com.signalcollect.deployment.DeploymentConfigurationCreator
import com.signalcollect.deployment.Cluster
import com.signalcollect.deployment.DeploymentConfiguration

import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus

class YarnCluster extends Cluster {
  var testDeployment = false
  lazy val yarnClient = YarnClientCreator.yarnClient
  var masterIp: String = "localhost"
  
  def setMasterIP(ip: String) {
    masterIp = ip
  }

  override def deploy(deploymentConfiguration: DeploymentConfiguration): Boolean = {
    val yarnDeploymentConfig = YarnDeploymentConfigurationCreator.getYarnDeploymentConfiguration(deploymentConfiguration)
    deploy(yarnDeploymentConfig)
  }
  
  def deploy(yarnDeploymentConfig: YarnDeploymentConfiguration): Boolean = {
    System.setProperty("HADOOP_USER_NAME", yarnDeploymentConfig.user)
    val algorithmClass = Class.forName(yarnDeploymentConfig.algorithm)
    val launchSettings = LaunchSettingsCreator.getSettingsForClass(algorithmClass, yarnDeploymentConfig, testDeployment)
    val launchSettingsWithMasterIp = launchSettings.copy(arguments = launchSettings.arguments :+ masterIp :+ yarnDeploymentConfig.algorithm)
    val client = new YarnDeploymentClient(launchSettingsWithMasterIp, yarnDeploymentConfig)
    val application = client.submitApplication()
    waitForTermination(application)
    val appReport = yarnClient.getApplications.toList.find(_.getApplicationId().equals(application)).get
    appReport.getFinalApplicationStatus() == FinalApplicationStatus.SUCCEEDED
  }

  def waitForTermination(application: ApplicationId) {
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
          if (applicationState == YarnApplicationState.FINISHED ||
            applicationState == YarnApplicationState.FAILED ||
            applicationState == YarnApplicationState.KILLED) {
            finished = true
          }
        }
      }
    }
  }

}


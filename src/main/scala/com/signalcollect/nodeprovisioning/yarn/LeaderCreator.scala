package com.signalcollect.nodeprovisioning.yarn

import com.signalcollect.util.ConfigProvider
import scala.collection.JavaConversions._
import com.signalcollect.util.DeploymentConfigurationCreator
import com.signalcollect.deployment.DeploymentConfiguration

object LeaderCreator {
  def getLeader(deploymentConfig: DeploymentConfiguration = DeploymentConfigurationCreator.getDeploymentConfiguration): Leader = {
    val config = ConfigProvider.config
    val baseport = config.getInt("deployment.akka.port")
    val akkaConfig = AkkaConfigCreator.getConfig(baseport)
    new DefaultLeader(akkaConfig = akkaConfig,
        deploymentConfig = deploymentConfig)
  }
}
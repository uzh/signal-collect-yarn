package com.signalcollect.deployment

import com.signalcollect.util.ConfigProvider
import com.signalcollect.deployment.DeploymentConfiguration
import com.signalcollect.deployment.DeploymentConfigurationCreator

object LeaderCreator {
  def getLeader(deploymentConfig: DeploymentConfiguration = DeploymentConfigurationCreator.getDeploymentConfiguration): Leader = {
    val config = ConfigProvider.config
    val baseport = config.getInt("deployment.akka.port")
    val akkaConfig = AkkaConfigCreator.getConfig(baseport)
    new DefaultLeader(akkaConfig = akkaConfig,
        deploymentConfig = deploymentConfig)
  }
}
package com.signalcollect.deployment

import com.signalcollect.util.ConfigProvider

object LeaderCreator {
  def getLeader(deploymentConfig: DeploymentConfiguration = DeploymentConfigurationCreator.getDeploymentConfiguration): Leader = {
    val config = ConfigProvider.config
    val baseport = config.getInt("deployment.akka.port")
    val akkaConfig = AkkaConfigCreator.getConfig(baseport)
    new DefaultLeader(akkaConfig = akkaConfig,
        deploymentConfig = deploymentConfig)
  }
}
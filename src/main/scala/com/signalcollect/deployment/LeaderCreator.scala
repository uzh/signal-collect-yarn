package com.signalcollect.deployment

import com.signalcollect.util.ConfigProvider
import com.signalcollect.deployment.DeploymentConfiguration
import com.signalcollect.deployment.DeploymentConfigurationCreator

object LeaderCreator {
  def getLeader(deploymentConfig: DeploymentConfiguration = DeploymentConfigurationCreator.getDeploymentConfiguration): Leader = {
    val baseport = deploymentConfig.akkaBasePort
    val akkaConfig = AkkaConfigCreator.getConfig(baseport, deploymentConfig)
    new DefaultLeader(akkaConfig = akkaConfig,
        deploymentConfig = deploymentConfig)
  }
}
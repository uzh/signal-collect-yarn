package com.signalcollect.yarn.deployment

import com.signalcollect.yarn.applicationmaster.ApplicationMaster
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions._
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import com.signalcollect.util.ConfigProvider
import com.signalcollect.deployment.DeploymentConfigurationCreator
import com.signalcollect.deployment.Cluster
import com.signalcollect.deployment.DeploymentConfiguration
import org.apache.hadoop.yarn.api.records.ApplicationId
import com.signalcollect.deployment.ClusterCreator

object YarnDeployer extends App {
  val deploymentConf = DeploymentConfigurationCreator.getDeploymentConfiguration
  val cluster = ClusterCreator.getCluster(deploymentConf)
  cluster.deploy(deploymentConf)

}


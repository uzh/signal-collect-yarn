package com.signalcollect.nodeprovisioning.yarn

import com.signalcollect.deployment.DeploymentConfigurationCreator

object LeaderApp extends App {
  val leader = LeaderCreator.getLeader(DeploymentConfigurationCreator.getDeploymentConfiguration)
  leader.start
}

object ContainerNodeApp extends App {
  val id = args(0).toInt
  val ip = args(1)
  val container = ContainerNodeCreator.getContainer(id = id, leaderIp = ip)
  try {
    container.start
  } finally {
    container.shutdown
  }
  System.exit(0) //ensures all Threads especially actorsystems are killed
}

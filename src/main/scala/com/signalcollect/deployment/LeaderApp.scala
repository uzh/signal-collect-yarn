package com.signalcollect.deployment

import com.signalcollect.configuration.ActorSystemRegistry
import com.signalcollect.deployment.DeploymentConfigurationCreator
import com.signalcollect.configuration.ActorSystemRegistry

object LeaderApp extends App {
  val leader = LeaderCreator.getLeader(DeploymentConfigurationCreator.getDeploymentConfiguration)
  leader.start
}

object ContainerNodeApp extends App {
//  NodeKiller.killOtherMasterAndNodes
 
  val id = args(0).toInt
  val ip = args(1)
  val container = ContainerNodeCreator.getContainer(id = id, leaderIp = ip)
  container.start
  container.waitForTermination
  println("execution terminated")
  val system = ActorSystemRegistry.retrieve("SignalCollect")
  if (system.isDefined) {
    if (!system.get.isTerminated) {
      system.get.shutdown
    }
  }
}

package com.signalcollect.nodeprovisioning.yarn

import com.signalcollect.deployment.DeploymentConfigurationCreator
import com.signalcollect.util.NodeKiller
import java.net.Inet4Address
import java.net.InetAddress

object LeaderApp extends App {
  val leader = LeaderCreator.getLeader(DeploymentConfigurationCreator.getDeploymentConfiguration)
  leader.start
}

object ContainerNodeApp extends App {
//  NodeKiller.killOtherMasterAndNodes
  val id = args(0).toInt
  val ip = args(1)
//  println("ip of container is:" + InetAddress.getLocalHost().getHostAddress())
  println("create container")
  val container = ContainerNodeCreator.getContainer(id = id, leaderIp = ip)
  println("start container")
  container.start
  println("waitForTermination")
  container.waitForTermination
  println("shutdown")
  if (container.isSuccessful) {
    System.exit(0) //ensures all Threads especially actorsystems are killed
  } else {
    System.exit(-1)
  }
}

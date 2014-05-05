package com.signalcollect.nodeprovisioning.yarn

object LeaderApp extends App {
  val leader = LeaderCreator.getLeader
  leader.start
}

object ContainerNodeApp extends App {
  val id = args(0).toInt
  val ip = args(1)
  val container = ContainerNodeCreator.getContainer(id = id, leaderIp = ip)
  container.start
}

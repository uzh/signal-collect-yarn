package com.signalcollect.nodeprovisioning.yarn

class ContainerNode(val ip: String, val id: Int, val akkaPort: Int = 2552) {
  def actorAddress: String = {
    val systemId = id + 1
    val containerPort = akkaPort + id + 1
    val address = s"""akka://SignalCollect$systemId@$ip:$containerPort/user/DefaultNodeActor$id"""
    address
  }
}
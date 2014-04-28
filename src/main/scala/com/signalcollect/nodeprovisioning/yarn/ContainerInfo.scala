package com.signalcollect.nodeprovisioning.yarn

/**
 * Every Container has an unique id and an actor system that runs on port 'baseport + id + 1'
 */
class ContainerInfo(val ip: String, val id: Int, val basePort: Int = 2552) {
  def actorAddress: String = {
    val systemId = id + 1
    val containerPort = basePort + id + 1
    val address = s"""akka://SignalCollect@$ip:$containerPort/user/DefaultNodeActor$id"""
    address
  }
}
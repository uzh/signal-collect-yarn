package com.signalcollect.nodeprovisioning.yarn

import org.junit.runner.RunWith
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ContainerNodeSpec() extends SpecificationWithJUnit {
  "ContainerNode" should {
    val ip = "0.0.0.0"
    val id = 0
    val akkaPort = 2552
    val node = new ContainerNode(ip = ip, id = id)
    "contain ip and id" in {
      node.ip === "0.0.0.0"
      node.id === 0
    }

    "create actorAddress" in {
      node.actorAddress === s"""akka://SignalCollect@$ip:$akkaPort/user/DefaultNodeActor$id"""
    }

    "create node with other port" in {
      val otherPort = 1111
      val node = new ContainerNode(ip = ip, id = id, akkaPort = otherPort)
      node.actorAddress === s"""akka://SignalCollect@$ip:$otherPort/user/DefaultNodeActor$id"""
    }

  }
}
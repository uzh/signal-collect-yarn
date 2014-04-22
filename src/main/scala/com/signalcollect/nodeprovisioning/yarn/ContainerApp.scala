package com.signalcollect.nodeprovisioning.yarn

import com.signalcollect.util.ConfigProvider
import com.signalcollect.util.LogHelper
import java.net.InetAddress

object ContainerApp extends App with LogHelper {
  val basePort = ConfigProvider.config.getInt("deployment.akka.port")
  val nodeId = args(0).toInt
  val ip = InetAddress.getLocalHost.getHostAddress()
  log.info("start DefaultActor on node " + nodeId + " with ip " + ip)
  val numberOfNodes = ConfigProvider.config.getInt("deployment.numberOfNodes")
  val nodeBootstrap = new YarnNodeBootstrap(nodeId, numberOfNodes, basePort)
  val actorRef = nodeBootstrap.startNode
  println("started")
  Thread.sleep(20000) // need to detect when execution is terminated
  log.info("stop Node")
  nodeBootstrap.stopNode
  System.exit(0)
}
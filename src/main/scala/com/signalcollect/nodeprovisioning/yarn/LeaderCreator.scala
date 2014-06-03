package com.signalcollect.nodeprovisioning.yarn

import com.signalcollect.util.ConfigProvider
import scala.collection.JavaConversions._

object LeaderCreator {
  def getLeader(): Leader = {
    val config = ConfigProvider.config
    val baseport = config.getInt("deployment.akka.port")
    val numberOfNodes = config.getInt("deployment.numberOfNodes")
    val akkaConfig = AkkaConfigCreator.getConfig(baseport)
    new DefaultLeader( numberOfNodes = numberOfNodes, akkaConfig = akkaConfig)
  }
}
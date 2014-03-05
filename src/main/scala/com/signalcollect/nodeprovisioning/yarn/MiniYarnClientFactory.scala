package com.signalcollect.nodeprovisioning.yarn

import com.typesafe.config.Config
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.MiniYARNCluster

class MiniYarnClientFactory extends YarnClientFactory {
	lazy val cluster = MiniCluster.getCluster()
  override def getYarnClient(config: Config): YarnClient = {
    val client = YarnClient.createYarnClient()
    client.init(cluster.getConfig())
    client.start()
    client
  }

  def stopMiniCluster() = {
     try {
        cluster.stop()
      }
  }
}
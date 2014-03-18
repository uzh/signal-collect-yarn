package com.signalcollect.yarn.deployment

import com.typesafe.config.Config
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.MiniYARNCluster
import com.signalcollect.util.ConfigProvider

class MiniYarnClientCreator extends YarnClientCreatorImpl {
  val config = ConfigProvider.config
  lazy val cluster = MiniCluster.getCluster()
  override def yarnClient(): YarnClient = {
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
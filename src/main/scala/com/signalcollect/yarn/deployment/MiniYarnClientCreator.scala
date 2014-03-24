package com.signalcollect.yarn.deployment

import com.typesafe.config.Config
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.MiniYARNCluster
import com.signalcollect.util.ConfigProvider
import org.apache.hadoop.conf.Configuration

class MiniYarnClientCreator extends YarnClientCreatorImpl {
  val config = ConfigProvider.config
  lazy val configuration = MiniCluster.getClusterConfig()
  override def yarnClient(): YarnClient = {
    val client = YarnClient.createYarnClient()
    val conf = new Configuration(configuration)
    println("RM Address: " + conf.get("yarn.resourcemanager.address"))
    println("classpath: " + conf.get("yarn.application.classpath"))
   
    client.init(conf)
    client.start()
    client
  }

  def stopMiniCluster() = {
    try {
      MiniCluster.cluster.stop()
    }
  }
}
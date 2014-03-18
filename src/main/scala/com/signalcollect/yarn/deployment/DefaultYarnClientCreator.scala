package com.signalcollect.yarn.deployment

import com.typesafe.config.Config
import com.signalcollect.util.LogHelper
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.client.api.YarnClient
import scala.collection.JavaConversions._
import com.signalcollect.util.ConfigProvider

class DefaultYarnClientCreator extends YarnClientCreatorImpl with LogHelper{
  val config = ConfigProvider.config
  override lazy val yarnClient = createYarnClient
  
  protected def isValidConfig(config: Config): Boolean = {
    if (config == null) false
    else if (config.hasPath("deployment.yarn.resourcemanager.host") &&
      config.hasPath("deployment.yarn.resourcemanager.address") &&
      config.hasPath("deployment.memory") &&
      config.hasPath("deployment.applicationName")) true
    else false
  }

  def createYarnClient: YarnClient = {
    if (!isValidConfig(config)) throw new IllegalArgumentException()
    val yarnOverrides = config.getConfig("deployment.yarn").entrySet().iterator()
    val yarnConfig = new YarnConfiguration()
    yarnOverrides.foreach(e => yarnConfig.set("yarn." + e.getKey(), e.getValue().unwrapped().toString()))
    yarnConfig.reloadConfiguration()
    createYarnClient(yarnConfig)
  }

  protected def createYarnClient(config: Configuration): YarnClient = {
    val yarnClient = YarnClient.createYarnClient()
    log.info("initialize YarnClient")
    yarnClient.init(config)
    yarnClient.start()
    yarnClient
  }

}
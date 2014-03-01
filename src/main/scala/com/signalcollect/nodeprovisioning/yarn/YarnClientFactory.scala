package com.signalcollect.nodeprovisioning.yarn

import com.typesafe.config.Config
import com.signalcollect.util.LogHelper
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.client.api.YarnClient
import scala.collection.JavaConversions._

object YarnClientFactory extends LogHelper{

  def isValidConfig(config: Config): Boolean = {
    if (config == null) false
    else if (config.hasPath("deployment.yarn.resourcemanager.host") &&
      config.hasPath("deployment.yarn.resourcemanager.address") &&
      config.hasPath("deployment.memory") &&
      config.hasPath("deployment.applicationName")) true
    else false
  }

  def getYarnClient(config: Config): YarnClient = {
    if (!isValidConfig(config: Config)) throw new IllegalArgumentException()
    val yarnOverrides = config.getConfig("deployment.yarn").entrySet().iterator()
    val yarnConfig = new YarnConfiguration()
    yarnOverrides.foreach(e => yarnConfig.set("yarn." + e.getKey(), e.getValue().unwrapped().toString()))
    yarnConfig.reloadConfiguration()
    createYarnClient(yarnConfig)
  }

  def createYarnClient(config: Configuration): YarnClient = {
    val yarnClient = YarnClient.createYarnClient()
    log.info("initialize YarnClient")
    yarnClient.init(config)
    yarnClient.start()
    yarnClient
  }

}
package com.signalcollect.nodeprovisioning.yarn

import com.typesafe.config.Config
import org.apache.hadoop.yarn.client.api.YarnClient

trait YarnClientFactory {
  
  def getYarnClient(config: Config): YarnClient
}
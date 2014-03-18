package com.signalcollect.yarn.deploy

import com.typesafe.config.Config
import org.apache.hadoop.yarn.client.api.YarnClient
import com.signalcollect.util.ConfigProvider

object YarnClientFactory {
  val config = ConfigProvider.config
  lazy val factory = createFactory
  
  def yarnClient(): YarnClient = {
    factory.yarnClient
  }
  
  def createFactory(): YarnClientFactoryImpl = {
    val factoryName = if (config.hasPath("deployment.factory.yarnclient")) config.getString("deployment.factory.yarnclient") else ""
    
    factoryName match {
      case "MiniYarnClientFactory" => new MiniYarnClientFactory
      case _ => new DefaultYarnClientFactory
    }
  }
}

trait YarnClientFactoryImpl{
  def yarnClient: YarnClient
}
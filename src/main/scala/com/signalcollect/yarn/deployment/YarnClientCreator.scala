package com.signalcollect.yarn.deployment

import com.typesafe.config.Config

import org.apache.hadoop.yarn.client.api.YarnClient

import com.signalcollect.util.ConfigProvider
import com.signalcollect.yarn.deployment.DefaultYarnClientCreator;
import com.signalcollect.yarn.deployment.MiniYarnClientCreator;

object YarnClientCreator {
  val config = ConfigProvider.config
  lazy val factory = createFactory
  
  def yarnClient(): YarnClient = {
    factory.yarnClient
  }
  
  def createFactory(): YarnClientCreatorImpl = {
    val factoryName = if (config.hasPath("deployment.factory.yarnclient")) config.getString("deployment.factory.yarnclient") else ""
    
    factoryName match {
      case "com.signalcollect.yarn.deployment.MiniYarnClientFactory" => new MiniYarnClientCreator
      case _ => new DefaultYarnClientCreator
    }
  }
}

trait YarnClientCreatorImpl{
  def yarnClient: YarnClient
}
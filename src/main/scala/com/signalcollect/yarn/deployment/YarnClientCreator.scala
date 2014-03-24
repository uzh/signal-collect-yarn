package com.signalcollect.yarn.deployment

import com.typesafe.config.Config

import org.apache.hadoop.yarn.client.api.YarnClient

import com.signalcollect.util.ConfigProvider

object YarnClientCreator {
  val config = ConfigProvider.config
  lazy val factory = createFactory
  
  def yarnClient(): YarnClient = {
    factory.yarnClient
  }
  
  def createFactory(): YarnClientCreatorImpl = {
    val useMiniCluster = if (config.hasPath("deployment.testing.useMiniCluster"))
      config.getBoolean("deployment.testing.useMiniCluster")
      else false
    
    if (useMiniCluster){
      new MiniYarnClientCreator
    } else {
      new DefaultYarnClientCreator
    }
  }
}

trait YarnClientCreatorImpl{
  def yarnClient: YarnClient
}
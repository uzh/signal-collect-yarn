package com.signalcollect.yarn.deployment

import com.typesafe.config.Config

import org.apache.hadoop.yarn.client.api.YarnClient

import com.signalcollect.util.ConfigProvider

object YarnClientCreator {
  val config = ConfigProvider.config
  var creator: YarnClientCreatorImpl = new DefaultYarnClientCreator
  var overrideFactory = false
  
  
  /**
   * This function allows to override the Creator which is chosen in the createFactory function
   * it is useful when running a container on the MiniCluster
   */
  def overrideFactory(factory: YarnClientCreatorImpl){
    overrideFactory = true
    creator = factory
  }
  
  def yarnClient(): YarnClient = {
    if(!overrideFactory){
    	creator = createFactory()
    }
    creator.yarnClient
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
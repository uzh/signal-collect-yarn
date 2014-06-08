/*
 *  @author Tobias Bachmann
 *
 *  Copyright 2014 University of Zurich
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.signalcollect.deployment.yarn

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
    val useMiniCluster = if (config.hasPath("testing.useMiniCluster"))
      config.getBoolean("testing.useMiniCluster")
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
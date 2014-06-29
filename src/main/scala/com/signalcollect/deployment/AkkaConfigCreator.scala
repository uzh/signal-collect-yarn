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

package com.signalcollect.deployment

import java.io.File

import com.signalcollect.configuration.AkkaConfig
import com.typesafe.config.Config

import akka.event.Logging

object AkkaConfigCreator {
  def getConfig(port: Int, deploymentConfig: DeploymentConfiguration): Config = {
    val log = new File("log_messages.txt");
    if (!log.exists()) {
      log.createNewFile()
    }

    val kryoRegistrations: List[String] = deploymentConfig.kryoRegistrations
    val kryoInit = deploymentConfig.kryoInit
    val serializeMessages = deploymentConfig.serializeMessages
    AkkaConfig.get(
      serializeMessages = serializeMessages,
      loggingLevel = Logging.DebugLevel, //Logging.DebugLevel,Logging.WarningLevel
      kryoRegistrations = kryoRegistrations,
      kryoInitializer = kryoInit,
      port = port)
  }
}
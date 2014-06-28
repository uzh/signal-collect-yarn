package com.signalcollect.deployment

import com.signalcollect.util.ConfigProvider
import scala.collection.JavaConversions._
import com.signalcollect.configuration.AkkaConfig
import akka.event.Logging
import com.typesafe.config.Config
import java.io.File

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
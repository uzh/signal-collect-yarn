package com.signalcollect.nodeprovisioning.yarn

import com.signalcollect.util.ConfigProvider
import scala.collection.JavaConversions._
import com.signalcollect.configuration.AkkaConfig
import akka.event.Logging
import com.typesafe.config.Config

object AkkaConfigCreator {
  def getConfig(port: Int): Config = {
    val config = ConfigProvider.config
    
    val kryoRegistrations: List[String] =
      if (config.hasPath("deployment.akka.kryo-registrations"))
        config.getStringList("deployment.akka.kryo-registrations").toList
      else List[String]()
    val kryoInit =
      if (config.hasPath("deployment.akka.kryo-initializer"))
        config.getString("deployment.akka.kryo-initializer")
      else ""
    val serializeMessages =
      if (config.hasPath("deployment.akka.serialize-messages"))
        config.getBoolean("deployment.akka.serialize-messages")
      else false
      AkkaConfig.get(
    serializeMessages = serializeMessages,
    loggingLevel = Logging.WarningLevel, //Logging.DebugLevel,Logging.WarningLevel
    kryoRegistrations = kryoRegistrations,
    kryoInitializer = kryoInit,
    port = port)
  }
}
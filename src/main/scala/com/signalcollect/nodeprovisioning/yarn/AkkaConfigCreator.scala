package com.signalcollect.nodeprovisioning.yarn

import com.signalcollect.util.ConfigProvider
import scala.collection.JavaConversions._
import com.signalcollect.configuration.AkkaConfig
import akka.event.Logging
import com.typesafe.config.Config
import java.io.File

object AkkaConfigCreator {
  def getConfig(port: Int): Config = {
    val log = new File("log_messages.txt");
    if (!log.exists()) {
      log.createNewFile()
    }
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
      loggingLevel = Logging.DebugLevel, //Logging.DebugLevel,Logging.WarningLevel
      kryoRegistrations = kryoRegistrations,
      kryoInitializer = kryoInit,
      port = port)
  }
}
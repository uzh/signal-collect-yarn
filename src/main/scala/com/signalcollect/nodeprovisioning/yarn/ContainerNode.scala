package com.signalcollect.nodeprovisioning.yarn

import akka.actor.ActorSystem
import com.signalcollect.configuration.ActorSystemRegistry
import com.signalcollect.configuration.AkkaConfig
import akka.event.Logging
import com.signalcollect.nodeprovisioning.AkkaHelper


class ContainerNode(id: Int, baseport: Int = 2552,  kryoRegistrations: List[String] = Nil, kryoInit: String = "com.signalcollect.configuration.KryoInit") {
  val system = ActorSystemRegistry.retrieve("SignalCollect").getOrElse(startActorSystem)
  
  val akkaPort = baseport + id + 1
    
  def startActorSystem: ActorSystem = {
    println("start Actorsystem")
    try {
      val system = ActorSystem("SignalCollect", akkaConfig(akkaPort, kryoRegistrations))
      ActorSystemRegistry.register(system)
      system
    } catch {
      case e: Exception => {
        throw e
      }
    }
  }
  
  def akkaConfig(akkaPort: Int, kryoRegistrations: List[String]) = AkkaConfig.get(
    akkaMessageCompression = true,
    serializeMessages = true,
    loggingLevel = Logging.WarningLevel, //Logging.DebugLevel,Logging.WarningLevel
    kryoRegistrations = kryoRegistrations,
    kryoInitializer = kryoInit,
    port = akkaPort)
}
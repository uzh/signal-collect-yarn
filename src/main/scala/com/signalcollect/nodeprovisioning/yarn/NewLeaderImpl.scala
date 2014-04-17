package com.signalcollect.nodeprovisioning.yarn

import com.signalcollect.configuration.ActorSystemRegistry
import akka.actor.ActorSystem
import com.signalcollect.util.LogHelper
import com.signalcollect.configuration.AkkaConfig
import akka.event.Logging
import akka.actor.Actor
import akka.actor.Props
import akka.actor.ActorRef

class NewLeaderImpl(akkaPort: Int, kryoRegistrations: List[String], kryoInitializer: String = "com.signalcollect.configuration.KryoInit") extends NewLeader with LogHelper {
  val system = ActorSystemRegistry.retrieve("SignalCollect").getOrElse(startActorSystem)

  def start {

  }

  def getActorRef(): ActorRef = {
    system.actorOf(Props[LeaderActor], "leaderactor")
  }
  
  

  def startActorSystem: ActorSystem = {
    try {
      val system = ActorSystem("SignalCollect", akkaConfig(akkaPort, kryoRegistrations))
      ActorSystemRegistry.register(system)
      system
    } catch {
      case e: Exception => {
        log.info("failed to start actor system: " + e.getMessage())
        throw e
      }
    }
  }

  def akkaConfig(akkaPort: Int, kryoRegistrations: List[String]) = AkkaConfig.get(
    akkaMessageCompression = true,
    serializeMessages = true,
    loggingLevel = Logging.WarningLevel, //Logging.DebugLevel,Logging.WarningLevel
    kryoRegistrations = kryoRegistrations,
    kryoInitializer = kryoInitializer,
    port = akkaPort)
}

class LeaderActor extends Actor {
  override def receive = {
    case address: String => NodeAddresses.add(address)
    case whatever => println("received unexpected Address")
  }
  def test {
    
  }
}

object NodeAddresses {
  private var addresses: List[String] = Nil
  def getAll: List[String] = {
    addresses
  }
  def add(address: String) {
    synchronized {
      addresses = address :: addresses
    }
  }
}



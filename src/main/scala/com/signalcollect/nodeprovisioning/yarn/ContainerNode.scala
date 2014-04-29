package com.signalcollect.nodeprovisioning.yarn

import akka.actor.ActorSystem
import com.signalcollect.configuration.ActorSystemRegistry
import com.signalcollect.configuration.AkkaConfig
import akka.event.Logging
import com.signalcollect.nodeprovisioning.AkkaHelper
import akka.actor.ActorRef
import akka.actor.Actor
import akka.actor.Props


class ContainerNode(id: Int, baseport: Int = 2552,  kryoRegistrations: List[String] = Nil, kryoInit: String = "com.signalcollect.configuration.KryoInit") {
  val system = ActorSystemRegistry.retrieve("Signval container = new ContainerNode(0)alCollect").getOrElse(startActorSystem)
  val akkaPort = baseport + id + 1
  
  var terminated = false
  
  def getShutdownActor(): ActorRef = {
    system.actorOf(Props[ShutdownActor], "shutdownactor")
  }
    
  def waitForTermination {
    
  }
  
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

class ShutdownActor extends Actor {
  override def receive = {
    case "shutdown" => ShutdownHelper.shutdown
    case whatever => println("received unexpected message")
  }
  def test {

  }
}

object ShutdownHelper {
  private var shutdownNow = false
  
  def shutdown = {
    synchronized {
      shutdownNow = true
    }
  }
  
  def isShutdownNow: Boolean = {
    shutdownNow
  }
}
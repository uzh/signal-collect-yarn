package com.signalcollect.nodeprovisioning.yarn

import akka.actor.ActorSystem
import com.signalcollect.configuration.AkkaConfig
import akka.event.Logging
import com.signalcollect.configuration.ActorSystemRegistry
import com.signalcollect.nodeprovisioning.NodeActorCreator
import akka.actor.Props
import com.signalcollect.nodeprovisioning.DefaultNodeActor

class YarnNodeBootstrap(
  akkaPort: Int = 2552,
  kryoRegistrations: List[String] = List.empty[String]) {
  def akkaConfig(akkaPort: Int, kryoRegistrations: List[String]) = AkkaConfig.get(
    akkaMessageCompression = true,
    serializeMessages = false,
    loggingLevel = Logging.WarningLevel, //Logging.DebugLevel,
    kryoRegistrations = kryoRegistrations,
    useJavaSerialization = false,
    port = akkaPort)

  def yarnExecutable {
    val nodeId = 1 // get container id?
    val numberOfNodes = 1 // how to get these?
    val system: ActorSystem = ActorSystem("SignalCollect", akkaConfig(akkaPort, kryoRegistrations))
    ActorSystemRegistry.register(system)
    val nodeControllerCreator = NodeActorCreator(nodeId, numberOfNodes, None)
    val nodeController = system.actorOf(Props[DefaultNodeActor].withCreator(
      nodeControllerCreator.create), name = "DefaultNodeActor" + nodeId.toString)
  }
}
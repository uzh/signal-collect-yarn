package com.signalcollect.nodeprovisioning.yarn

import java.io.PrintWriter
import java.net.InetAddress
import java.text.SimpleDateFormat
import java.util.Calendar
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.event.Logging
import com.signalcollect.configuration.AkkaConfig
import com.signalcollect.nodeprovisioning.NodeActorCreator
import com.signalcollect.nodeprovisioning.AkkaHelper
import com.signalcollect.configuration.ActorSystemRegistry
import com.signalcollect.nodeprovisioning.DefaultNodeActor
import akka.actor.ActorRef

class YarnNodeBootstrap(nodeId: Int,
  numberOfNodes: Int,
  basePort: Int = 2552,
  kryoRegistrations: List[String] = List.empty[String],
  kryoInit: String = "com.signalcollect.configuration.KryoInit") {
  
  val nodePort = basePort + nodeId + 1
  val system: ActorSystem = ActorSystemRegistry.retrieve("SignalCollect").getOrElse(startActorSystem)
  val nodeControllerCreator = NodeActorCreator(nodeId, numberOfNodes, None)
  val nodeController = system.actorOf(Props[DefaultNodeActor].withCreator(
    nodeControllerCreator.create), name = "DefaultNodeActor" + nodeId.toString)

  def akkaConfig(port: Int, kryoRegistrations: List[String]) = AkkaConfig.get(
    akkaMessageCompression = true,
    serializeMessages = true,
    loggingLevel = Logging.DebugLevel, //Logging.DebugLevelLogging.WarningLevel,
    kryoRegistrations = kryoRegistrations,
    kryoInitializer = kryoInit,
    port = port)

  def startNode: ActorRef = {
    //    val nodeControllerCreator = NodeActorCreator(nodeId, numberOfNodes, None)
    //    val nodeController = system.actorOf(Props[DefaultNodeActor].withCreator(
    //      nodeControllerCreator.create), name = "DefaultNodeActor" + nodeId.toString)

    val address = AkkaHelper.getRemoteAddress(nodeController, system)
    nodeController ! "test"
    println("test send")
    val cal = Calendar.getInstance()
    cal.getTime()
    val sdf = new SimpleDateFormat("HH:mm:ss:SSS")
    val time = sdf.format(cal.getTime())

    val message = time + " : actor system on node started with port: " + nodePort + " address: " + address
    println(message)
    
    nodeController

  }

  def startActorSystem: ActorSystem = {
    println("start actorSystem on port " + nodePort)
    val system = ActorSystem("SignalCollect", akkaConfig(nodePort, kryoRegistrations))
    ActorSystemRegistry.register(system)
    system
  }

  def stopNode {
    system.shutdown
  }
}
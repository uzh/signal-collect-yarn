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
  kryoRegistrations: List[String] = List.empty[String]) {
  val out = new PrintWriter("/home/tobi/log.txt")
  val nodePort = basePort + nodeId + 1
  val system: ActorSystem = ActorSystemRegistry.retrieve("SignalCollect" + (nodeId + 1)).getOrElse(startActorSystem)
  val nodeControllerCreator = NodeActorCreator(nodeId, numberOfNodes, None)
  val nodeController = system.actorOf(Props[DefaultNodeActor].withCreator(
    nodeControllerCreator.create), name = "DefaultNodeActor" + nodeId.toString)

  def akkaConfig(port: Int, kryoRegistrations: List[String]) = AkkaConfig.get(
    akkaMessageCompression = true,
    serializeMessages = true,
    loggingLevel = Logging.DebugLevel, //Logging.DebugLevelLogging.WarningLevel,
    kryoRegistrations = kryoRegistrations,
    useJavaSerialization = true,
    port = port)

  def startNode: ActorRef = {
    //    val nodeControllerCreator = NodeActorCreator(nodeId, numberOfNodes, None)
    //    val nodeController = system.actorOf(Props[DefaultNodeActor].withCreator(
    //      nodeControllerCreator.create), name = "DefaultNodeActor" + nodeId.toString)

    val address = AkkaHelper.getRemoteAddress(nodeController, system)
    nodeController ! "test"
    out.println("test send")
    val cal = Calendar.getInstance()
    cal.getTime()
    val sdf = new SimpleDateFormat("HH:mm:ss:SSS")
    val time = sdf.format(cal.getTime())

    val ip = InetAddress.getLocalHost.getHostAddress()
    val message = time + " : actor system on node started with port: " + nodePort + " and Ip: " + ip + " address: " + address
    println(message)
    out.println(message)
    
    nodeController

  }

  def startActorSystem: ActorSystem = {
    println("start actorSystem on port " + nodePort)
    val system = ActorSystem("SignalCollect" + (nodeId + 1), akkaConfig(nodePort, kryoRegistrations))
    ActorSystemRegistry.register(system)
    out.println("actor system on node started")
    system
  }

  def stopNode {
    system.shutdown
    out.close
  }
}
///*
// *  @author Tobias Bachmann
// *
// *  Copyright 2014 University of Zurich
// *
// *  Licensed under the Apache License, Version 2.0 (the "License");
// *  you may not use this file except in compliance with the License.
// *  You may obtain a copy of the License at
// *
// *         http://www.apache.org/licenses/LICENSE-2.0
// *
// *  Unless required by applicable law or agreed to in writing, software
// *  distributed under the License is distributed on an "AS IS" BASIS,
// *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// *  See the License for the specific language governing permissions and
// *  limitations under the License.
// *
// */
//package com.signalcollect.nodeprovisioning.yarn
//
//import java.io.PrintWriter
//import java.net.InetAddress
//import java.text.SimpleDateFormat
//import java.util.Calendar
//import akka.actor.ActorSystem
//import akka.actor.Props
//import akka.actor.actorRef2Scala
//import akka.event.Logging
//import com.signalcollect.configuration.AkkaConfig
//import com.signalcollect.nodeprovisioning.NodeActorCreator
//import com.signalcollect.nodeprovisioning.AkkaHelper
//import com.signalcollect.configuration.ActorSystemRegistry
//import com.signalcollect.nodeprovisioning.DefaultNodeActor
//import akka.actor.ActorRef
//
//class YarnNodeBootstrap(nodeId: Int,
//  numberOfNodes: Int,
//  basePort: Int = 2552,
//  kryoRegistrations: List[String] = List.empty[String],
//  kryoInit: String = "com.signalcollect.configuration.KryoInit") {
//  
//  val nodePort = basePort + nodeId + 1
//  val system: ActorSystem = ActorSystemRegistry.retrieve("SignalCollect").getOrElse(startActorSystem)
//  val nodeControllerCreator = NodeActorCreator(nodeId, numberOfNodes, None)
//  val nodeController = system.actorOf(Props[DefaultNodeActor].withCreator(
//    nodeControllerCreator.create), name = "DefaultNodeActor" + nodeId.toString)
//
//  def akkaConfig(port: Int, kryoRegistrations: List[String]) = AkkaConfig.get(
//    akkaMessageCompression = true,
//    serializeMessages = true,
//    loggingLevel = Logging.WarningLevel, //Logging.DebugLevel Logging.WarningLevel,
//    kryoRegistrations = kryoRegistrations,
//    kryoInitializer = kryoInit,
//    port = port)
//
//  def startNode: ActorRef = {
//    //    val nodeControllerCreator = NodeActorCreator(nodeId, numberOfNodes, None)
//    //    val nodeController = system.actorOf(Props[DefaultNodeActor].withCreator(
//    //      nodeControllerCreator.create), name = "DefaultNodeActor" + nodeId.toString)
//
//    val address = AkkaHelper.getRemoteAddress(nodeController, system)
//    nodeController ! "test"
//    println("test send")
//    val cal = Calendar.getInstance()
//    cal.getTime()
//    val sdf = new SimpleDateFormat("HH:mm:ss:SSS")
//    val time = sdf.format(cal.getTime())
//
//    val message = time + " : actor system on node started with port: " + nodePort + " address: " + address
//    println(message)
//    
//    nodeController
//
//  }
//
//  def startActorSystem: ActorSystem = {
//    println("start actorSystem on port " + nodePort)
//    val system = ActorSystem("SignalCollect", akkaConfig(nodePort, kryoRegistrations))
//    ActorSystemRegistry.register(system)
//    system
//  }
//
//  def stopNode {
//    system.shutdown
//  }
//}
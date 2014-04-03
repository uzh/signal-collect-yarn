package com.signalcollect.nodeprovisioning.yarn

import akka.event.Logging
import akka.actor.ActorSystem
import akka.actor.ActorPath

object AkkaHelloClient extends App {
  val akkaConfig = AkkaConfigYarn.get(
    akkaMessageCompression = false,
    serializeMessages = false,
    loggingLevel = Logging.DebugLevel,
    kryoRegistrations = List("java.lang.String", "com.signalcollect.nodeprovisioning.yarn.Mooh"),
    useJavaSerialization = false,
    port = 3001)
  val system = ActorSystem("Client", akkaConfig)
  try {
    val actorAddress = s"akka://Server@127.0.0.1:3000/user/helloactor"
    println(s"Hello actor address: $actorAddress")
    //val helloActor = system.actorFor(actorAddress)
    val path = ActorPath.fromString(actorAddress)
    val helloActor = system.actorFor(path)
    helloActor ! (new Mooh)
  } finally {
    system.shutdown
  }
}
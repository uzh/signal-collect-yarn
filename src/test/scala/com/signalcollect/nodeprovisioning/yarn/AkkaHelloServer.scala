package com.signalcollect.nodeprovisioning.yarn

import akka.actor.ActorSystem
import akka.event.Logging
import com.signalcollect.nodeprovisioning.AkkaHelper
import akka.actor.Props
import akka.actor.Actor

object AkkaHelloServer extends App {
  val akkaConfig = AkkaConfigYarn.get(
    akkaMessageCompression = false,
    serializeMessages = false,
    loggingLevel = Logging.DebugLevel,
    kryoRegistrations = List("java.lang.String", "com.signalcollect.nodeprovisioning.yarn.Mooh"),
    useJavaSerialization = false,
    port = 3000)
  val system = ActorSystem("Server", akkaConfig)
  val helloActor = system.actorOf(Props[HelloActor], name = "helloactor")
  val remoteHelloActorAddress = AkkaHelper.getRemoteAddress(helloActor, system)
  println(s"Hello actor is now available @ $remoteHelloActorAddress")
  try {
    while (true) {
      Thread.sleep(100)
    }
  } finally {
    system.shutdown
  }
}


  
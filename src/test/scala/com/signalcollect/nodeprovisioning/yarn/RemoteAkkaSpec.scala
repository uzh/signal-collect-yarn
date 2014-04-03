package com.signalcollect.nodeprovisioning.yarn

import org.junit.runner.RunWith
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.runner.JUnitRunner
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import java.net.InetAddress
import java.io.{ InputStreamReader, BufferedReader }
import System.{ getProperty => Prop }
import akka.actor.Actor
import akka.actor.Props
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.async.Async.{async, await}
import akka.event.Logging
import com.signalcollect.configuration.AkkaConfig


@RunWith(classOf[JUnitRunner])
class RemoteAkkaSpec extends SpecificationWithJUnit {
  "RemoteAkkaSpec" should {

    "run on 2 Jvms" in {
      val host = InetAddress.getLocalHost.getHostAddress
      val port = 2552
      val otherPort = port + 1
      val system = ActorSystemCreator.createSystem(host, port)
      val actorAddress = s"akka://ActorSystem@127.0.0.1:$otherPort/user/helloactor"
      println(s"Hello actor address: $actorAddress")
      val helloActor = system.actorFor(actorAddress)

      async {
        ProcessSpawner.spawn(ContainerBootstrap.getClass().getCanonicalName().dropRight(1), true)
      }
      Thread.sleep(2000) //wait for container to be started
      try {
        var i = 0
        while (i < 1) {
          helloActor ! (new Mooh)
          i += 1
          Thread.sleep(1000)
        }
      } finally {
    	  Thread.sleep(1000)
    	  system.shutdown

      }
      0 === 0
    }

  }

}

class Mooh

object ActorSystemCreator {
  def createSystem(host: String, akkaPort: Int): ActorSystem = {
    val akkaConfig = AkkaConfig.get( akkaMessageCompression= false,
    serializeMessages = false,
    loggingLevel = Logging.DebugLevel,
    kryoRegistrations= Nil,
    useJavaSerialization = false,
    port = akkaPort)
    ActorSystem("ActorSystem", akkaConfig)
  }
}

object ContainerBootstrap extends App {
  println("started ContainerBootstrap")
  val host = InetAddress.getLocalHost.getHostAddress
  val port = 2553
  println("start ActorSystem")
  val system = ActorSystemCreator.createSystem(host, port)
  system.actorOf(Props[HelloActor], name = "helloactor")
  Thread.sleep(10000)
  system.shutdown
}

class HelloActor extends Actor {

  println("Hello actor is alive!")

  def receive = {
    case whatever => println(s"Hello actor received $whatever")
  }
}

object ProcessSpawner {
  val sep = Prop("file.separator")
  val classpath = Prop("java.class.path") + ":./target/scala-2.10/signal-collect-assembly-1.0-SNAPSHOT.jar"
  println(classpath)
  val path = Prop("java.home") + sep + "bin" + sep + "java"

  def spawn(className: String,
    redirectStream: Boolean) {
    val processBuilder = new ProcessBuilder(path, "-cp", classpath, className)
    val pbcmd = processBuilder.command().toString()

    processBuilder.redirectErrorStream(redirectStream)

    val process = processBuilder.start()
    val reader = new BufferedReader(new InputStreamReader(process.getInputStream()))

    var line: String = ""
    while ((line) != null) {
      println(s"[$className]$line")
      line = reader.readLine()
    }
    reader.close()
    process.waitFor()
  }

}

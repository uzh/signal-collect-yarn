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

@RunWith(classOf[JUnitRunner])
class RemoteAkkaSpec extends SpecificationWithJUnit {
  "RemoteAkkaSpec" should {

    "run on 2 Jvms" in {

      async {
        ProcessSpawner.spawn(ContainerBootstrap.getClass().getCanonicalName().dropRight(1), true)
      }
      val host = InetAddress.getLocalHost.getHostAddress
      val port = 2552
      val otherPort = port + 1
      val system = ActorSystemCreator.createSystem(host, port)
      Thread.sleep(2000) //wait for container to be started
      val helloActor = system.actorFor(s"akka://SignalCollect@$host:$otherPort/user/helloactor")
      try {
    	  helloActor ! "hello" must not(throwAn[Exception])
      } finally {
    	  system.shutdown
      }
      0 === 0
    }

  }

}
object ActorSystemCreator {
  def createSystem(host: String, port: Int): ActorSystem = {
    val akkaConfig = ConfigFactory.parseString(
      s"""akka {
				actor {
				provider = "akka.remote.RemoteActorRefProvider"
				}
				remote {
				transport = "akka.remote.netty.NettyRemoteTransport"
				netty {
				hostname = "$host"
				port = $port
				}
				}
				}""")
      .withFallback(ConfigFactory.load)
    ActorSystem("ActorSystem", akkaConfig)
  }
}

object ContainerBootstrap extends App {
  println("started ContainerBootstrap")
  val host = InetAddress.getLocalHost.getHostAddress
  val port = 2553
  println("start ActorSystem")
  val system = ActorSystemCreator.createSystem(host, port)
  Thread.sleep(10000)
  system.shutdown
}

class HelloActor extends Actor {
  def receive = {
    case "hello" => println("hello back at you")
    case _       => println("huh?")
  }
}

object ProcessSpawner {
  val sep = Prop("file.separator")
  val classpath = Prop("java.class.path") + ":./target/scala-2.10/signal-collect-yarn-assembly-1.0-SNAPSHOT.jar"
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
    while((line) != null){
      println(s"[$className]$line")
      line = reader.readLine()
    }
    reader.close()
    process.waitFor()
  }

}
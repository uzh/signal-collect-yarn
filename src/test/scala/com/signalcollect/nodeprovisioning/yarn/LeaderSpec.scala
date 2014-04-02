package com.signalcollect.nodeprovisioning.yarn

import org.junit.runner.RunWith
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.runner.JUnitRunner
import java.net.InetAddress
import com.signalcollect.yarn.deployment.JarCreator
import com.signalcollect.yarn.deployment.LocalJarRunner

@RunWith(classOf[JUnitRunner])
class LeaderSpec() extends SpecificationWithJUnit {
//  "Leader" should {
//    val akkaPort = 2552
//    "throw Exception when list is empty" in {
//      val emptyIps = List[ContainerNode]()
//      new Leader(emptyIps, akkaPort, List[String]()) must throwAn[IllegalArgumentException]
//    }
//
//    val ip = InetAddress.getLocalHost.getHostAddress
//    val id = 0
//    val nodes = List[ContainerNode](new ContainerNode(ip, 0))
//    val nodeBootstrap = new YarnNodeBootstrap(id, 1)
//    val leader = new Leader(nodes, akkaPort, List[String]())
//
//    "start execution" in {
////      val node = new Thread(new Runnable {
////        def run() {
////          val klass = ContainerApp.getClass()
////          val pathToJar = JarCreator.createJarFile(klass)
////          val fullClassName = klass.getName()
////          val className = fullClassName.substring(0, fullClassName.length() - 1)
////          LocalJarRunner.run(pathToJar, className, id.toString)
////          Thread.sleep(20000)
////          System.exit(0)
////        }
////      })
////      node.start()
//            nodeBootstrap.startNode
//      Thread.sleep(10000) //wait till node is up 
//      leader.startExecution must not(throwAn[Exception])
//    }
//  
//
////    "start execution in single jvm" in {
////
////      nodeBootstrap.startNode
////      Thread.sleep(1000) //wait till node is up 
////      leader.startExecution must not(throwAn[Exception])
////    }

  }
}
package com.signalcollect.nodeprovisioning.yarn

import com.signalcollect.deployment.DeploymentConfigurationCreator
import com.signalcollect.util.NodeKiller
import java.net.Inet4Address
import java.net.InetAddress
import com.signalcollect.configuration.ActorSystemRegistry

object LeaderApp extends App {
  val leader = LeaderCreator.getLeader(DeploymentConfigurationCreator.getDeploymentConfiguration)
  leader.start
}

object ContainerNodeApp extends App {
//  NodeKiller.killOtherMasterAndNodes
 
  val id = args(0).toInt
  val ip = args(1)
  val container = ContainerNodeCreator.getContainer(id = id, leaderIp = ip)
  container.start
  container.waitForTermination
  println("execution terminated")
  val system = ActorSystemRegistry.retrieve("SignalCollect")
  if (system.isDefined) {
    if (!system.get.isTerminated) {
      system.get.shutdown
    }
  }
}

object MemoryUsage {
  def print {
    new Thread(new Runnable {
      def run() {
        while (true) {
          Thread.sleep(2000)
          val runtime = Runtime.getRuntime()
          val maxMemory = runtime.maxMemory()
          val allocatedMemory = runtime.totalMemory()
          val freeMemory = runtime.freeMemory()
          println(s"maxMemory: $maxMemory")
          println(s"allocatedMemory: $allocatedMemory")
          println(s"freeMemory: $freeMemory")
        }
      }
    }).start()
  }
}

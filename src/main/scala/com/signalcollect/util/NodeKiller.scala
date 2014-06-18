package com.signalcollect.util

import scala.sys.process._
import java.lang.management.ManagementFactory

object NodeKiller extends App {
  killOtherMasterAndNodes
 
  def killOtherMasterAndNodes {
    val ownPID = ManagementFactory.getRuntimeMXBean().getName().split("@").head
    println(ownPID)
    val jps: String = "jps".!!.toString
    println(jps)
    val process = jps.split("\\n").map(p => (p.split("\\s+")(0), p.split("\\s+")(1))).toList
    val processesToKill = process.filter(p => p._2.contains("ContainerNode") || p._2.contains("ApplicationMaster")).map(_._1)
    val excludeOwnProcess = processesToKill.filterNot(p => p == ownPID)
    println(processesToKill)
    excludeOwnProcess.foreach(p => println(s"kill -9 $p".!!))
    Thread.sleep(1000)
    println("jps".!!.toString)
  }
}
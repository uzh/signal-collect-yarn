package com.signalcollect.util

import scala.sys.process._
import java.lang.management.ManagementFactory

/**
 * This Helper kills other instances of the app, which still run as zombies
 */
object NodeKiller extends App {
  killOtherMasterAndNodes
 
  def killOtherMasterAndNodes {
    val ownPID = ManagementFactory.getRuntimeMXBean().getName().split("@").head
    val jps: String = "jps".!!.toString
    println(jps)
    val process = jps.split("\\n").map(p => (p.split("\\s+")(0), p.split("\\s+")(1))).toList
    val processesToKill = process.filter(p => p._2.contains("NodeContainer") || p._2.contains("ApplicationMaster")).map(_._1)
    val excludeOwnProcess = processesToKill.filterNot(p => p == ownPID)
    excludeOwnProcess.foreach(p => println(s"process to kill $p"))
    excludeOwnProcess.foreach(p => s"kill -9 $p".!!)
    Thread.sleep(3000)
  }
}
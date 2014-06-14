package com.signalcollect.util

import scala.sys.process._

object NodeKiller extends App{
val jps: String ="jps".!!.toString
println(jps)
val process = jps.split("\\n").map( p => (p.split("\\s+")(0),p.split("\\s+")(1))).toList
val processesToKill = process.filter(p => p._2.contains("ContainerNode") || p._2.contains("ApplicationMaster")).map(_._1)
 println(processesToKill)
processesToKill.foreach(p => println(s"kill -9 $p".!!))
Thread.sleep(1000)
println("jps".!!.toString)
}
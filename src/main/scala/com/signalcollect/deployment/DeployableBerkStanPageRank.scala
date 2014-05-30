package com.signalcollect.deployment

import java.io.BufferedReader
import java.io.FileInputStream
import java.io.InputStreamReader

import com.signalcollect.Graph
import com.signalcollect.GraphBuilder
import com.signalcollect.examples.PageRankEdge
import com.signalcollect.examples.PageRankVertex

import akka.actor.ActorRef

class DeployableBerkStanPageRank extends YarnDeployableAlgorithm {
  def execute(parameters: Map[String, String], nodeActors: Array[ActorRef]) {
    val graph = GraphBuilder.withPreallocatedNodes(nodeActors).withShutdownActorSystem(false).build
    val filename = "web-BerkStan.txt"
    val in = new BufferedReader(new InputStreamReader(new FileInputStream(filename)))
    var line = in.readLine
    while (line != null) {
      if (!line.startsWith("#")) {
        val edgeTuple = line.split("	")
        val nextSource = toInt(edgeTuple(0))
        val nextTarget = toInt(edgeTuple(1))
        addVerticesAndEdge(nextSource, nextTarget, graph)
      }
      line = in.readLine
    }

    println("Graph has been built, awaiting idle ...")
    graph.awaitIdle
    println("Executing computation ...")
    val stats = graph.execute
    println(stats)
    graph.shutdown

  }
  def addVerticesAndEdge(source: Int, target: Int,graph: Graph[Any,Any]) {
	  graph.addVertex(new PageRankVertex(source))
	  graph.addVertex(new PageRankVertex(target))
	  graph.addEdge(source, new PageRankEdge(target))
  }
  
  def toInt(s: String): Int = {
		  Integer.valueOf(s)
  }
}
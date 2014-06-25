/*
 *  @author Philip Stutz
 *  @author Tobias Bachmann
 *  
 *  Copyright 2014 University of Zurich
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.signalcollect.deployment

import java.io.BufferedReader
import java.io.FileReader
import java.io.InputStreamReader
import java.net.URL

import com.signalcollect.Edge
import com.signalcollect.GraphBuilder
import com.signalcollect.GraphEditor
import com.signalcollect.Vertex
import com.signalcollect.examples.EfficientPageRankVertex
import com.signalcollect.examples.PlaceholderEdge
import com.signalcollect.factory.messagebus.BulkAkkaMessageBusFactory

/** Builds a PageRank compute graph and executes the computation */
class TwitterGraph extends DeployableAlgorithm {
  override def execute(parameters: Map[String, String], graphBuilder: GraphBuilder[Any, Any]) {
    println("download graph")
    val url = parameters.get("url").getOrElse("https://s3-eu-west-1.amazonaws.com/signalcollect/user/hadoop/twitterSmall.txt")
    //    FileDownloader.downloadFile(new URL(url), "twitter_rv.net")
    Thread.sleep(1000)
    println("build graph")
    val graph = graphBuilder.
      withMessageBusFactory(new BulkAkkaMessageBusFactory(10000, false)).
      //      withAkkaMessageCompression(false).
      withHeartbeatInterval(100).
      withEagerIdleDetection(false).
      withThrottlingEnabled(true).
      build
    println("set handlers")
    graph.setEdgeAddedToNonExistentVertexHandler {

      Handlers.nonExistingVertex

    }
    graph.setUndeliverableSignalHandler {
      Handlers.undeliverableSignal
    }
    println("await idle")
    graph.awaitIdle
    println("read file and load graph")
    val beginTime = System.currentTimeMillis()
    val numberOfNodes = parameters.get("number-of-readers").getOrElse("1").toInt
    val length = parameters.get("length-data").getOrElse("1000").toLong
    println(s"nrOfNodes = $numberOfNodes, length = $length")
    var id = 0
    for( id  <- 0 until numberOfNodes){
      println(s"load graph with hint $id")
      graph.loadGraph(getFileLoader(length, numberOfNodes, id, url), Some(id*8))
    }
  
    println("loading graph")
    graph.awaitIdle
    val end = System.currentTimeMillis() - beginTime
    println(s"file read in $end ms")
    println("execute")
    val stats = graph.execute //(ExecutionConfiguration.withExecutionMode(ExecutionMode.Interactive))
    println(stats)
    graph.shutdown
    Thread.sleep(100)

  }
  def getRange(length: Long, numberOfNodes: Int, id: Int): (Long, Long) = {
    val splitLength = length / numberOfNodes
    val start = id * splitLength
    val end = if (id != numberOfNodes - 1) start + splitLength else length
    (start, end)
  }
  
  def getFileLoader(length: Long, numberOfNodes: Int, id: Int, url: String):Iterator[GraphEditor[Any, Any] => Unit] = {
    val range = getRange(length,numberOfNodes,id)
    FileLoader(url, range._1 , range._2).asInstanceOf[Iterator[GraphEditor[Any, Any] => Unit]]
  }
}

case class FileLoader(fileUrl: String, start: Long, end: Long) extends Iterator[GraphEditor[Int, Double] => Unit] {
  lazy val in = getBufferedReader
  var cnt = start
  var edgeCnt = 0
  val startTime = System.currentTimeMillis
  def addEdge(vertices: (Vertex[Int, _], Vertex[Int, _]))(graphEditor: GraphEditor[Int, Double]) {
    //    graphEditor.addVertex(vertices._1)
    //    graphEditor.addVertex(vertices._2)
    graphEditor.addEdge(vertices._2.id, new PlaceholderEdge[Int](vertices._1.id).asInstanceOf[Edge[Int]])
  }
  
  var notEndOfFile = true
  
  def hasNext = {
    val hasItNext = cnt < end && notEndOfFile
    hasItNext
  }
  
  var line = ""
  
  def next: GraphEditor[Int, Double] => Unit = {
    in
    cnt += line.length() + 1
    edgeCnt += 1
    val edge = line.split("\\s").map(_.toInt)
    val target = edge(0)
    val source = edge(1)
    val vertices = (new EfficientPageRankVertex(edge(0)), new EfficientPageRankVertex(edge(1)))
    if (edgeCnt % 1000000 == 0) {
      val currentTime = System.currentTimeMillis() - startTime
      println("")
      println(s"read $edgeCnt edges in $currentTime ms")
      println("free memory is" + Runtime.getRuntime.freeMemory() + "from " + Runtime.getRuntime.maxMemory())
    }
    line = in.readLine()
    if( line == null){
      notEndOfFile = false
    }
    addEdge(vertices) _
  }

  def getBufferedReader: BufferedReader = {
    println("create Reader")
    val url = new URL(fileUrl)
    val conn = url.openConnection()
    val is = conn.getInputStream()
    conn.setReadTimeout(3000000)
    conn.setConnectTimeout(3000000)
    val reader = new BufferedReader(new InputStreamReader(is, "UTF-8"))
    println("skip " + start)
    skipInSteps(reader, start)
//    reader.skip(start)
    if (start != 0) {
      cnt += reader.readLine().length
      cnt += line.length
    }
    line = reader.readLine()
    reader
  }
  
  def skipInSteps(reader:BufferedReader, skip: Long) {
    val step = 50000000L
    var currentSkip = step
    while( currentSkip < skip) {
      reader.skip(step)
      println("skipped: " + currentSkip)
      currentSkip += step
    }
    reader.skip(skip - currentSkip + step)
  }

}

/*
 *  @author Philip Stutz
*   @author Daniel Strebel
 *
 *  Copyright 2013 University of Zurich
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
import java.io._
import java.net.URL
import java.nio.channels.Channels
import java.util.zip.GZIPInputStream
import scala.collection.mutable.ArrayBuffer
import com.signalcollect.util.Ints
import akka.actor.ActorRef

/**
 * Downloads a web graph and splits it such that it can be loaded in parallel.
 */
object DeployableGraphSplitter extends YarnDeployableAlgorithm {
  
   override def execute(parameters: Map[String, String], nodeActors: Array[ActorRef]) {
     println("executing GraphSplitter")
	   val webGraphUrl = new URL("http://snap.stanford.edu/data/web-BerkStan.txt.gz")
	   val webGraphZip = "web-BerkStan.txt.gz"
	   val decompressedName = "web-BerkStan.txt"
	   println("Downloading ...")
	   downloadFile(webGraphUrl, webGraphZip)
	   println("Decompressing ...")
	   decompressGzip(webGraphZip, decompressedName)
	   println("Splitting ...")
	   splitGraph(decompressedName, Runtime.getRuntime.availableProcessors)
	   println("Done")
   }

  def downloadFile(url: URL, localFileName: String) {
    val in = Channels.newChannel(url.openStream)
    val out = new FileOutputStream(localFileName)
    out.getChannel.transferFrom(in, 0, Int.MaxValue)
    in.close
    out.close
  }

  def decompressGzip(archive: String, decompressedName: String) {
    val zin = new GZIPInputStream(new FileInputStream(archive))
    val os = new FileOutputStream(decompressedName)
    val buffer = new Array[Byte](2048)
    var read = zin.read(buffer)
    while (read > 0) {
      os.write(buffer, 0, read)
      read = zin.read(buffer)
    }
    os.close
    zin.close
  }

  /**
   * Parses the Berkeley/Stanford web graph format, splits the vertices up among multiple
   * files and encodes them as:
   * firstVertexId numberOfEdges edge1 edge2 edge3 scondVertexId numberOfEdges edge1 edge2 ...
   * The ids are assumed to be unsigned Ints and written as VarInts.
   */
  def splitGraph(fileName: String, splitFactor: Int) {
    var vertexMap = Map[Int, ArrayBuffer[Int]]()
    val outStreams = {
      val files = new Array[DataOutputStream](splitFactor)
      for (i <- 0 until files.length) {
        files(i) = new DataOutputStream(new FileOutputStream(s"web-split-$i"))
      }
      files
    }
    val in = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)))
    var line = in.readLine
    while (line != null) {
      if (!line.startsWith("#")) {
        val edgeTuple = line.split("	")
        val nextSource = toInt(edgeTuple(0))
        val nextTarget = toInt(edgeTuple(1))
        if (vertexMap.contains(nextSource)) {
          val existingEdges = vertexMap(nextSource)
          existingEdges.append(nextTarget)
        } else {
          vertexMap += nextSource -> ArrayBuffer[Int](nextTarget)
        }
        if (!vertexMap.contains(nextTarget)) {
          vertexMap += nextTarget -> ArrayBuffer[Int]()
        }
      }
      if (vertexMap.size % 10000 == 0) {
        println(vertexMap.size + " vertices read ...")
      }
      line = in.readLine
    }
    println("Writing ...")
    for (vertexId <- vertexMap.keys) {
      write(vertexId, vertexMap(vertexId))
    }
    in.close
    outStreams foreach (_.close)
    def toInt(s: String): Int = {
      Integer.valueOf(s)
    }
    def write(vertexId: Int, edges: Traversable[Int]) {
      val index = vertexId.hashCode % splitFactor
      val out = outStreams(index)
      Ints.writeUnsignedVarInt(vertexId, out)
      Ints.writeUnsignedVarInt(edges.size, out)
      edges foreach {
        Ints.writeUnsignedVarInt(_, out)
      }
    }
  }

}
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

package com.signalcollect.deployment.examples

import com.signalcollect.deployment.DeployableAlgorithm
import com.signalcollect.util.FileDownloader
import com.signalcollect.GraphBuilder
import java.net.URL
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.async.Async.{ async, await }

import scala.sys.process._

/** Builds a PageRank compute graph and executes the computation */
class TwitterGraph extends DeployableAlgorithm {
  override def execute(parameters: Map[String, String], graphBuilder: GraphBuilder[Any, Any]) {
    println("download graph")
    FileDownloader.downloadFile(new URL("https://s3-eu-west-1.amazonaws.com/signalcollect/user/hadoop/twitterSmall.txt.gz"), "twitter.txt.gz")
    println("decompress graph")
    FileDownloader.decompressGzip("twitter.txt.gz", "twitter.txt")
    
  }
}

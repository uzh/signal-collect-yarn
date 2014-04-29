/*
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
package com.signalcollect.nodeprovisioning.yarn

import java.net.InetAddress
import org.junit.runner.RunWith
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.runner.JUnitRunner
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.async.Async.{ async, await }
import com.signalcollect.yarn.deployment.JarCreator
import com.signalcollect.yarn.deployment.LocalJarRunner

@RunWith(classOf[JUnitRunner])
class LeaderSpec() extends SpecificationWithJUnit {
//  "Leader" should {
////    val akkaPort = 2552
////    "throw Exception when list is empty" in {
////      val emptyIps = List[ContainerInfo]()
////      new Leader(emptyIps, akkaPort, List[String]()) must throwAn[IllegalArgumentException]
////    }
////
////    val ip = InetAddress.getLocalHost.getHostAddress
////    val id = 0
////    val nodes = List[ContainerInfo](new ContainerInfo(ip, 0), new ContainerInfo(ip, 1))
////    val leader = new Leader(nodes, akkaPort, List[String]())
////
////    "start execution" in {
////      async {
////        startContainer(0)
////      }
////      async {
////        startContainer(1)
////      }
////
////      Thread.sleep(10000) //wait till nodes are up 
////      leader.startExecution must not(throwAn[Exception])
////    }
////
////    def startContainer(id: Int): Unit = {
////      val klass = ContainerApp.getClass
////      val pathToJar = JarCreator.createJarFile(klass)
////      val fullClassName = klass.getName
////      val className = fullClassName.substring(0, fullClassName.length - 1)
////      LocalJarRunner.run(pathToJar, className, id.toString)
////      Thread.sleep(1000)
////    }
// 
//
//  }
}
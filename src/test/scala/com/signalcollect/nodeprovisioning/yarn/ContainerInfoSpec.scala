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

import org.junit.runner.RunWith
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ContainerInfoSpec() extends SpecificationWithJUnit {
  sequential
  "ContainerNode" should {
    println("Test executing now: ContainerInfoSpec")
    val ip = "0.0.0.0"
    val id = 0
    val basePort = 2552
    val port = basePort + 1
    val node = new ContainerInfo(ip = ip, id = id)
    "contain ip and id" in {
      node.ip === "0.0.0.0"
      node.id === 0
    }

    "create actorAddress" in {
      node.actorAddress === s"""akka.tcp://SignalCollect@$ip:$port/user/DefaultNodeActor$id"""
    }

    "create node with other baseport" in {
      val otherPort = 1111
      val port = otherPort + 1
      val node = new ContainerInfo(ip = ip, id = id, basePort = otherPort)
      node.actorAddress === s"""akka.tcp://SignalCollect@$ip:$port/user/DefaultNodeActor$id"""
    }

  }
}
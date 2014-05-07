/**
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

/**
 * Every Container has an unique id and an actor system that runs on port 'baseport + id + 1'
 */
case class ContainerInfo(val ip: String, val id: Int, val basePort: Int = 2552) {
  def actorAddress: String = {
    val systemId = id + 1
    val containerPort = basePort + id + 1
    val address = s"""akka://SignalCollect@$ip:$containerPort/user/DefaultNodeActor$id"""
    address
  }
}
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

import com.signalcollect.util.ConfigProvider
import scala.collection.JavaConversions._

object ContainerNodeCreator {
 def getContainer(id: Int, leaderIp: String): ContainerNode =  {
   val config = ConfigProvider.config
   val basePort = config.getInt("deployment.akka.port")
   val numberOfNodes = config.getInt("deployment.numberOfNodes")
   val kryoRegistration: List[String] =
      if (config.hasPath("deployment.akka.kryo-registrations"))
        config.getStringList("deployment.akka.kryo-registrations").toList
      else List[String]()
    val kryoInit =
      if (config.hasPath("deployment.akka.kryo-initializer"))
        config.getString("deployment.akka.kryo-initializer")
      else ""
   val container = new DefaultContainerNode(id= id,
  numberOfNodes = numberOfNodes,
  leaderIp = leaderIp,
  basePort = basePort,
  kryoRegistrations = kryoRegistration,
  kryoInit = kryoInit)
  container
 }
}
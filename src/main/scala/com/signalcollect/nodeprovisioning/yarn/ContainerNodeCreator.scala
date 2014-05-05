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